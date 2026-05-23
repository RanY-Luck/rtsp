"""
stream_proxy.py —— DJI 机巢视频流预热代理
==========================================
解决 startLive 接口 3 秒延迟问题。

原理：
  ① 启动时后台预调所有机巢的 startLive 接口，缓存返回的 FLV 地址
  ② 同时对每个 FLV 地址建立保活连接，让流服务器推流进程始终在线
  ③ Java 点播时代理直接命中缓存返回，无需等待

部署（Java 代码零改动）：
  1. pip install -r requirements.txt
  2. python stream_proxy.py
  3. 浏览器打开 http://<本机IP>:9000/ 用管理 UI 添加机巢 deviceId
  4. 把 Java/前端配置文件里的服务地址端口改为本机 9000
     原：http://172.29.0.14:8888/api/proxy/djVideo/startLive
     改：http://<本机IP>:9000/api/proxy/djVideo/startLive
"""

import asyncio
import os
import threading
import time
import logging
import sys
import json
import re
import aiohttp
import uvicorn
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response, JSONResponse, HTMLResponse, StreamingResponse
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("dji-proxy")

# ╔══════════════════════════════════════════════════════╗
# ║                    【配置区】                        ║
# ╚══════════════════════════════════════════════════════╝

# Java 后端服务地址（原始地址，不改）
UPSTREAM = "http://172.29.0.14:8888"

# 本代理监听端口（Java/前端改用这个端口）
PROXY_PORT = 9000

# 机巢 deviceId 持久化文件（可通过环境变量 NESTS_FILE 覆盖）
# 容器内默认挂载到 /app/data/nests.json，宿主机用 ./data 卷
NESTS_FILE = os.environ.get("NESTS_FILE", "/app/data/nests.json")

# OAuth2 token 持久化文件（access_token / refresh_token / 过期时间）
TOKENS_FILE = os.environ.get("TOKENS_FILE", "/app/data/tokens.json")

# OAuth2 登录接口 URL（drone 网关 /auth/oauth2/token）
AUTH_TOKEN_URL = os.environ.get(
    "AUTH_TOKEN_URL", UPSTREAM + "/api/auth/oauth2/token"
)

# 引导用的 refresh_token / Basic 客户端凭证（首次启动或失效时通过 UI / env 配置）
# AUTH_CLIENT_BASIC 形如 "Basic ZHJvbmVfbmRrZ2J3Omp..." 或纯 base64
BOOTSTRAP_REFRESH_TOKEN = os.environ.get("BOOTSTRAP_REFRESH_TOKEN", "").strip()
AUTH_CLIENT_BASIC = os.environ.get("AUTH_CLIENT_BASIC", "").strip()

# 提前刷新阈值：access_token 剩余有效期不足这个秒数时主动刷新
TOKEN_REFRESH_AHEAD_SECONDS = 300

# 接口缓存有效期（秒），超时后后台自动刷新
CACHE_TTL = 90

# FLV 保活带宽控制（每路约 8KB/s，15路共约 120KB/s）
FLV_CHUNKS_PER_SECOND = 2
FLV_CHUNK_SIZE = 4096

# ╔══════════════════════════════════════════════════════╗
# ║                    内部实现                          ║
# ╚══════════════════════════════════════════════════════╝

# 接口相关常量（已从抓包确认，无需修改）
STREAM_API_PATH = "/api/proxy/djVideo/startLive"
VIDEO_TYPE = 1  # 固定参数
FLV_JSON_KEY = "data"  # 响应 JSON 里 FLV 地址所在字段
RECONNECT_DELAY = 5


# 运行时机巢列表（启动时从 NESTS_FILE 加载，UI 增删后持久化回去）
NEST_DEVICE_IDS: List[str] = []
_nest_names: Dict[str, str] = {}  # deviceId -> 用户起的名字（可选）
_nests_lock = threading.Lock()

# OAuth2 token 状态（启动时从 TOKENS_FILE 加载，刷新成功后持久化回去）
_auth: Dict[str, object] = {
    "access_token": "",
    "refresh_token": "",
    "expires_at": 0,  # unix timestamp
    "last_refreshed_at": 0,
    "last_error": "",
    "client_basic": "",  # 形如 "Basic xxxx"，刷 token 时作为 Authorization 头
}
_auth_lock: Optional[asyncio.Lock] = None  # 在 lifespan 里创建（需要事件循环）


@dataclass
class CacheEntry:
    status_code: int
    headers: dict
    body: bytes
    flv_url: Optional[str]
    timestamp: float = field(default_factory=time.time)

    def age(self) -> float:
        return time.time() - self.timestamp

    def is_valid(self) -> bool:
        return self.age() < CACHE_TTL


_cache: Dict[str, CacheEntry] = {}  # deviceId -> CacheEntry
_api_status: Dict[str, str] = {}  # deviceId -> "warming"|"ready"|"failed"
_flv_alive: Dict[str, str] = {}  # flv_url  -> "alive"|"dead"|"connecting"
_flv_tasks: Dict[str, "asyncio.Task"] = {}  # flv_url -> asyncio 保活任务
_session: Optional[aiohttp.ClientSession] = None


# ── 持久化 ─────────────────────────────────────────────

def load_nests() -> List[Dict[str, str]]:
    """从 NESTS_FILE 读取机巢列表，返回 [{deviceId, name}, ...]。
    兼容旧格式 list[str]，自动迁移到新格式 list[{deviceId, name}]。"""
    path = Path(NESTS_FILE)
    if not path.exists():
        return []
    try:
        with _nests_lock:
            data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            log.warning(f"{NESTS_FILE} 格式异常（顶层不是数组），忽略")
            return []
        result = []
        for item in data:
            if isinstance(item, str) and item:
                # 旧格式：纯 deviceId 字符串
                result.append({"deviceId": item, "name": ""})
            elif isinstance(item, dict) and item.get("deviceId"):
                # 新格式：{deviceId, name}
                result.append({
                    "deviceId": str(item["deviceId"]),
                    "name": str(item.get("name", "") or ""),
                })
        return result
    except Exception as e:
        log.warning(f"读取 {NESTS_FILE} 失败: {e}")
        return []


def save_nests(nests: List[Dict[str, str]]) -> None:
    """把机巢列表（list[{deviceId, name}]）原子写回 NESTS_FILE"""
    path = Path(NESTS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with _nests_lock:
        tmp.write_text(json.dumps(nests, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)


def _persist_nests() -> None:
    """把当前内存中的 NEST_DEVICE_IDS + _nest_names 持久化"""
    save_nests([
        {"deviceId": d, "name": _nest_names.get(d, "")}
        for d in NEST_DEVICE_IDS
    ])


# ── OAuth2 token 管理 ──────────────────────────────────

def _normalize_client_basic(v: str) -> str:
    """把用户粘进来的 client 凭证规范化为 'Basic xxx' 形式"""
    v = (v or "").strip()
    if not v:
        return ""
    if v.lower().startswith("basic "):
        return "Basic " + v[6:].strip()
    return "Basic " + v


def load_tokens() -> None:
    """启动时从 TOKENS_FILE 加载已持久化的 token 状态"""
    path = Path(TOKENS_FILE)
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            for k in ("access_token", "refresh_token", "expires_at",
                      "last_refreshed_at", "client_basic"):
                if k in data:
                    _auth[k] = data[k]
            log.info(f"📂 已加载 token 状态 (expires_at={_auth['expires_at']})")
        except Exception as e:
            log.warning(f"读取 {TOKENS_FILE} 失败: {e}")

    # 环境变量优先级低于已持久化的状态，仅在持久化为空时生效（首次引导）
    if not _auth.get("refresh_token") and BOOTSTRAP_REFRESH_TOKEN:
        _auth["refresh_token"] = BOOTSTRAP_REFRESH_TOKEN
        log.info("🔑 已从环境变量 BOOTSTRAP_REFRESH_TOKEN 引导 refresh_token")
    if not _auth.get("client_basic") and AUTH_CLIENT_BASIC:
        _auth["client_basic"] = _normalize_client_basic(AUTH_CLIENT_BASIC)
        log.info("🔑 已从环境变量 AUTH_CLIENT_BASIC 引导客户端凭证")


def save_tokens() -> None:
    """把 token 状态原子写回 TOKENS_FILE"""
    path = Path(TOKENS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = {k: _auth[k] for k in ("access_token", "refresh_token",
                                   "expires_at", "last_refreshed_at",
                                   "client_basic")}
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


async def refresh_access_token() -> None:
    """用当前 refresh_token 调 /oauth2/token 拿新的 access_token。
    成功后更新 _auth + 持久化；失败抛异常。"""
    if not _auth.get("refresh_token"):
        raise RuntimeError("refresh_token 未配置 —— 请在 UI 引导一次")
    if not _auth.get("client_basic"):
        raise RuntimeError("client_basic 未配置 —— 请在 UI 引导一次")
    form = {
        "grant_type": "refresh_token",
        "refresh_token": _auth["refresh_token"],
    }
    headers = {
        "Authorization": _auth["client_basic"],
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        async with session().post(
            AUTH_TOKEN_URL, data=form, headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {text[:300]}")
            body = json.loads(text)
    except Exception as e:
        msg = str(e) or e.__class__.__name__
        _auth["last_error"] = msg
        log.warning(f"🔐 刷新 token 失败: {msg}")
        raise RuntimeError(msg) from e

    access = body.get("access_token", "")
    if not access:
        _auth["last_error"] = f"响应中没有 access_token: {text[:200]}"
        raise RuntimeError(_auth["last_error"])
    _auth["access_token"] = access
    # refresh_token 可能轮换；服务器没返回新的就保留旧的
    if body.get("refresh_token"):
        _auth["refresh_token"] = body["refresh_token"]
    expires_in = int(body.get("expires_in", 43200))
    _auth["expires_at"] = int(time.time()) + expires_in
    _auth["last_refreshed_at"] = int(time.time())
    _auth["last_error"] = ""
    save_tokens()
    log.info(f"🔐 token 续期成功，{expires_in}s 后过期")


async def ensure_token() -> str:
    """返回一个保证有效的 access_token，必要时自动续期。"""
    now = int(time.time())
    if _auth.get("access_token") and _auth.get("expires_at", 0) > now + TOKEN_REFRESH_AHEAD_SECONDS:
        return _auth["access_token"]
    # 串行化续期，避免并发刷新
    assert _auth_lock is not None
    async with _auth_lock:
        now = int(time.time())
        if _auth.get("access_token") and _auth.get("expires_at", 0) > now + TOKEN_REFRESH_AHEAD_SECONDS:
            return _auth["access_token"]
        await refresh_access_token()
        return _auth["access_token"]


def invalidate_token() -> None:
    """收到 401 时调用，强制下次请求重新续期"""
    _auth["access_token"] = ""
    _auth["expires_at"] = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 创建 asyncio lock（必须在事件循环里创建）
    global _auth_lock
    _auth_lock = asyncio.Lock()
    # 加载持久化的 token 状态
    load_tokens()
    # 启动：从持久化文件加载机巢列表
    loaded = load_nests()
    NEST_DEVICE_IDS.clear()
    _nest_names.clear()
    for n in loaded:
        NEST_DEVICE_IDS.append(n["deviceId"])
        _nest_names[n["deviceId"]] = n["name"]
    log.info(f"📂 从 {NESTS_FILE} 加载机巢 {len(NEST_DEVICE_IDS)} 个")
    # 并发预热（每个预热完成会自己起保活协程）+ 后台周期刷新
    asyncio.create_task(prewarm_all())
    asyncio.create_task(bg_refresh())
    yield
    # 关闭：取消所有保活任务
    tasks = list(_flv_tasks.values())
    for t in tasks:
        if not t.done():
            t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    # 释放 HTTP session
    if _session and not _session.closed:
        await _session.close()


app = FastAPI(lifespan=lifespan)


def session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


def extract_flv(body: bytes) -> Optional[str]:
    """从响应 JSON 的 data 字段提取 FLV 地址"""
    try:
        data = json.loads(body)
        url = data.get(FLV_JSON_KEY, "")
        if url and ".flv" in url:
            return url
    except Exception:
        pass
    # 降级：正则兜底
    match = re.search(
        r'https?://[^\s\'"]+\.flv[^\s\'"]*',
        body.decode("utf-8", errors="ignore")
    )
    return match.group(0) if match else None


# ── 接口预热 ───────────────────────────────────────────

async def _call_startlive(device_id: str, access_token: str) -> aiohttp.ClientResponse:
    """单次调用上游 startLive，返回原始 aiohttp 响应对象（调用方负责读取/关闭）"""
    payload = json.dumps({"deviceId": device_id, "videoType": VIDEO_TYPE})
    headers = {"Content-Type": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    return await session().post(
        UPSTREAM + STREAM_API_PATH,
        data=payload,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=20),
    )


async def prewarm(device_id: str) -> bool:
    _api_status[device_id] = "warming"
    # 先确保 token 有效
    try:
        token = await ensure_token()
    except Exception as e:
        _api_status[device_id] = "failed"
        log.warning(f"❌ 预热失败 [{device_id[-8:]}...] 鉴权问题: {e}")
        return False

    # 记下旧 flv_url，用于在 URL 变化时停掉旧保活（防止线程/任务泄漏）
    old_entry = _cache.get(device_id)
    old_flv_url = old_entry.flv_url if old_entry else None

    try:
        async with await _call_startlive(device_id, token) as resp:
            # 收到 401 → 强制续期 + 重试一次
            if resp.status == 401:
                log.warning(f"⚠️ 收到 401，强制续期后重试 [{device_id[-8:]}...]")
                invalidate_token()
                token = await ensure_token()
                resp.release()
                async with await _call_startlive(device_id, token) as resp2:
                    body = await resp2.read()
                    status = resp2.status
                    resp_headers = dict(resp2.headers)
            else:
                body = await resp.read()
                status = resp.status
                resp_headers = dict(resp.headers)

        flv_url = extract_flv(body)
        _cache[device_id] = CacheEntry(status, resp_headers, body, flv_url)
        if status == 200 and flv_url:
            _api_status[device_id] = "ready"
            log.info(f"✅ 预热完成 [{device_id[-8:]}...] → {flv_url}")
            # URL 变了 → 停掉旧保活，避免任务累积
            if old_flv_url and old_flv_url != flv_url:
                stop_keepalive(old_flv_url)
                log.info(f"🔄 FLV URL 变化，停掉旧保活 → ...{old_flv_url[-30:]}")
            start_keepalive(flv_url)
            return True
        else:
            _api_status[device_id] = "failed"
            log.warning(f"❌ 预热失败 [{device_id[-8:]}...] HTTP {status} body={body[:200]!r}")
            return False
    except Exception as e:
        _api_status[device_id] = "failed"
        log.warning(f"❌ 预热失败 [{device_id[-8:]}...] → {e}")
        return False


async def prewarm_all():
    ids = list(NEST_DEVICE_IDS)
    if not ids:
        log.info("🔥 当前没有机巢，跳过预热（请通过 UI 添加）")
        return
    log.info(f"🔥 并发预热 {len(ids)} 个机巢...")
    results = await asyncio.gather(
        *[prewarm(d) for d in ids], return_exceptions=True
    )
    ok = sum(1 for r in results if r is True)
    log.info(f"预热完成：{ok}/{len(ids)} 成功")


# ── FLV 保活（asyncio 协程）─────────────────────────────
# 每路 FLV 流跑在一个 asyncio.Task 里（不再起 OS 线程），事件循环统一调度。
# N 个机巢 → 主进程依然只占少量线程，彻底消除 "can't start new thread"。

async def flv_worker_async(flv_url: str):
    try:
        while True:
            _flv_alive[flv_url] = "connecting"
            try:
                # connect=10s 短超时（探活），sock_read/total=None 长流模式不限时
                timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=None)
                async with session().get(
                        flv_url,
                        headers={"User-Agent": "FLV-KeepAlive/1.0"},
                        timeout=timeout,
                ) as r:
                    if r.status != 200:
                        log.warning(f"[FLV] {r.status} → {flv_url}")
                        _flv_alive[flv_url] = "dead"
                        await asyncio.sleep(RECONNECT_DELAY)
                        continue
                    _flv_alive[flv_url] = "alive"
                    log.info(f"[FLV] 保活建立 → {flv_url}")
                    chunks = 0
                    async for chunk in r.content.iter_chunked(FLV_CHUNK_SIZE):
                        if chunk:
                            chunks += 1
                            if chunks > 50:  # 初始帧读完后节流
                                await asyncio.sleep(1.0 / FLV_CHUNKS_PER_SECOND)
                    log.warning(f"[FLV] 流断开，重连 → {flv_url}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"[FLV] 异常: {e}")
            _flv_alive[flv_url] = "dead"
            await asyncio.sleep(RECONNECT_DELAY)
    except asyncio.CancelledError:
        log.info(f"[FLV] 收到取消，退出保活 → {flv_url}")
        raise
    finally:
        _flv_alive.pop(flv_url, None)
        _flv_tasks.pop(flv_url, None)


def start_keepalive(flv_url: str):
    """启动一个 asyncio 保活任务（同一 URL 重复调用是幂等的）。
    必须从 async 上下文调用（需要 running event loop）。"""
    if not flv_url:
        return
    if flv_url in _flv_tasks:
        return  # 已经在跑，不重复创建
    task = asyncio.create_task(
        flv_worker_async(flv_url),
        name=f"flv-{flv_url[-16:]}"
    )
    _flv_tasks[flv_url] = task


def stop_keepalive(flv_url: str):
    """取消 flv_url 对应的保活任务（同步函数，cancel 后立即返回）"""
    if not flv_url:
        return
    task = _flv_tasks.pop(flv_url, None)
    if task and not task.done():
        task.cancel()
    _flv_alive.pop(flv_url, None)


# ── 后台刷新 ───────────────────────────────────────────

async def bg_refresh():
    await asyncio.sleep(20)
    while True:
        try:
            ids = list(NEST_DEVICE_IDS)
            stale = [d for d in ids
                     if d not in _cache
                     or not _cache[d].is_valid()
                     or _api_status.get(d) == "failed"]
            if stale:
                log.info(f"♻️ 刷新 {len(stale)} 个过期机巢")
                await asyncio.gather(
                    *[prewarm(d) for d in stale],
                    return_exceptions=True
                )

            ready = sum(1 for d, s in _api_status.items()
                        if d in ids and s == "ready")
            alive = sum(1 for s in _flv_alive.values() if s == "alive")
            log.info(
                f"── 状态 接口缓存:{ready}/{len(ids)}  "
                f"FLV保活:{alive}/{len(_flv_alive)}"
            )
        except Exception as e:
            log.error(f"刷新异常: {e}")
        await asyncio.sleep(30)


# ── 管理 UI / Admin API ────────────────────────────────
# 注意：所有具名路由必须放在最下方 catch-all 之前，否则会被代理拦截

INDEX_HTML = """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>DJI 机巢代理 — 管理</title>
<script src="https://cdn.jsdelivr.net/npm/flv.js@1.6.2/dist/flv.min.js"></script>
<style>
  * { box-sizing: border-box; }
  body { font-family: -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif;
         margin: 0; padding: 24px; background: #f5f5f7; color: #1d1d1f; }
  h1 { margin: 0 0 8px; font-size: 22px; }
  .sub { color: #6e6e73; margin-bottom: 24px; font-size: 13px; }
  .auth-bar { background: #fff; padding: 14px 18px; border-radius: 8px; margin-bottom: 16px;
              display: flex; align-items: center; gap: 16px; box-shadow: 0 1px 2px rgba(0,0,0,.05); }
  .auth-bar.warn { background: #fff8e6; border: 1px solid #f0c850; }
  .auth-bar.err { background: #ffefef; border: 1px solid #f0a8a8; }
  .auth-bar .auth-state { font-size: 13px; font-weight: 600; padding: 4px 10px; border-radius: 4px; }
  .auth-bar.ok .auth-state { background: #e4f8e8; color: #1d8a3a; }
  .auth-bar.warn .auth-state { background: #fff4d6; color: #8a5a00; }
  .auth-bar.err .auth-state { background: #ffe1e0; color: #c1271c; }
  .auth-bar .meta { flex: 1; font-size: 12px; color: #6e6e73; line-height: 1.6; }
  .auth-bar .meta code { background: #f0f0f2; padding: 1px 5px; border-radius: 3px; font-size: 11px; }
  .stats { display: flex; gap: 16px; margin-bottom: 20px; }
  .stat { background: #fff; padding: 12px 18px; border-radius: 8px; flex: 0 0 auto;
          box-shadow: 0 1px 2px rgba(0,0,0,.05); }
  .stat .n { font-size: 22px; font-weight: 600; }
  .stat .l { font-size: 12px; color: #6e6e73; }
  .add { background: #fff; padding: 16px; border-radius: 8px; margin-bottom: 20px;
         display: flex; gap: 8px; box-shadow: 0 1px 2px rgba(0,0,0,.05); }
  .add input { flex: 1; padding: 8px 12px; border: 1px solid #d2d2d7; border-radius: 6px;
               font-size: 14px; font-family: monospace; }
  .add input:disabled { background: #f5f5f7; color: #aaa; }
  .add button, .add button:hover { background: #0071e3; color: #fff; border: none;
                  padding: 8px 18px; border-radius: 6px; font-size: 14px; cursor: pointer; }
  .add button:disabled { background: #c0c0c4; cursor: not-allowed; }
  table { width: 100%; background: #fff; border-radius: 8px; border-collapse: collapse;
          overflow: hidden; box-shadow: 0 1px 2px rgba(0,0,0,.05); }
  th, td { padding: 10px 12px; text-align: left; font-size: 13px;
           border-bottom: 1px solid #f0f0f0; }
  th { background: #fafafa; font-weight: 600; color: #6e6e73; font-size: 12px;
       text-transform: uppercase; letter-spacing: .3px; }
  tr:last-child td { border-bottom: none; }
  td.id { font-family: monospace; }
  td.flv { font-family: monospace; max-width: 260px; overflow: hidden;
           text-overflow: ellipsis; white-space: nowrap; color: #6e6e73; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px;
           font-weight: 600; }
  .b-ready, .b-alive { background: #e4f8e8; color: #1d8a3a; }
  .b-warming, .b-connecting { background: #fff4d6; color: #8a5a00; }
  .b-failed, .b-dead { background: #ffe1e0; color: #c1271c; }
  .b-pending, .b-null { background: #ebebeb; color: #6e6e73; }
  td.actions { text-align: right; white-space: nowrap; }
  .btn { background: #f5f5f7; border: 1px solid #d2d2d7; border-radius: 5px;
         padding: 4px 10px; font-size: 12px; cursor: pointer; margin-left: 4px; }
  .btn:hover { background: #ebebed; }
  .btn-del { color: #c1271c; }
  .btn-primary { background: #0071e3; color: #fff; border-color: #0071e3; }
  .btn-primary:hover { background: #005bb5; }
  .empty { padding: 40px; text-align: center; color: #6e6e73; }
  /* modal */
  .mask { position: fixed; inset: 0; background: rgba(0,0,0,.4); display: none;
          align-items: center; justify-content: center; z-index: 10; }
  .mask.show { display: flex; }
  .modal { background: #fff; border-radius: 10px; padding: 24px; width: 560px; max-width: 90vw;
           max-height: 90vh; overflow-y: auto; box-shadow: 0 8px 30px rgba(0,0,0,.15); }
  .modal h2 { margin: 0 0 8px; font-size: 18px; }
  .modal .help { color: #6e6e73; font-size: 12px; margin-bottom: 16px; line-height: 1.6; }
  .modal label { display: block; font-size: 12px; font-weight: 600; margin: 12px 0 4px;
                 color: #6e6e73; text-transform: uppercase; letter-spacing: .3px; }
  .modal textarea { width: 100%; padding: 8px 10px; border: 1px solid #d2d2d7; border-radius: 6px;
                    font-family: monospace; font-size: 12px; resize: vertical; min-height: 60px; }
  .modal .actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 18px; }
  .modal .err-msg { color: #c1271c; font-size: 12px; margin-top: 8px; min-height: 16px; }
  /* player */
  .player-modal { width: 760px; max-width: 92vw; }
  .player-modal video { width: 100%; background: #000; border-radius: 6px;
                        max-height: 60vh; display: block; }
  .player-modal .player-info { color: #6e6e73; font-size: 12px; margin: 10px 0;
                               word-break: break-all; font-family: monospace; }
  .btn-play { background: #34c759; color: #fff; border-color: #34c759; }
  .btn-play:hover { background: #2eaf4d; }
  .btn-rename { padding: 2px 6px; font-size: 11px; opacity: .6; }
  .btn-rename:hover { opacity: 1; }
  td.name { font-weight: 600; }
  td.name .name-text { margin-right: 4px; }
  td.name i { font-weight: normal; color: #aaa; font-style: normal; }
  .add #device-name { flex: 0 0 180px; font-family: -apple-system, "PingFang SC", sans-serif; }
</style>
</head>
<body>
  <h1>DJI 机巢视频流代理</h1>
  <div class="sub">在下方输入机巢 deviceId 即可加入预热列表，添加后立即开始预热 + FLV 保活。</div>

  <div id="auth-bar" class="auth-bar"><span class="auth-state" id="a-state">加载中…</span>
    <div class="meta" id="a-meta"></div>
    <button class="btn btn-primary" id="btn-auth">配置鉴权</button>
    <button class="btn" id="btn-auth-refresh" title="手动触发一次刷新">立即续期</button>
  </div>

  <div class="stats">
    <div class="stat"><div class="n" id="s-total">-</div><div class="l">机巢总数</div></div>
    <div class="stat"><div class="n" id="s-ready">-</div><div class="l">缓存就绪</div></div>
    <div class="stat"><div class="n" id="s-alive">-</div><div class="l">FLV 保活中</div></div>
  </div>

  <form class="add" id="add-form">
    <input id="device-id" placeholder="deviceId，例如 7CTDM4L00B2KBG" autocomplete="off" required>
    <input id="device-name" placeholder="名称（可选，便于识别）" autocomplete="off">
    <button type="submit">添加</button>
  </form>

  <table>
    <thead><tr>
      <th>名称</th><th>deviceId</th><th>接口</th><th>FLV 地址</th><th>保活</th><th>缓存年龄</th><th></th>
    </tr></thead>
    <tbody id="tbody"><tr><td colspan="7" class="empty">加载中…</td></tr></tbody>
  </table>

  <div class="mask" id="auth-modal">
    <div class="modal">
      <h2>配置鉴权（粘贴一次即可，之后自动续期）</h2>
      <div class="help">
        1. 用浏览器正常登录这个 DJI 系统，<b>F12 打开 DevTools → Network</b><br>
        2. 找到登录请求（一般叫 <code>oauth2/token</code>）<br>
        3. <b>Authorization 头</b> 复制到下方第一个框（形如 <code>Basic ZHJvbmVfbmRrZ2J3Omp...</code>）<br>
        4. <b>响应里的 refresh_token</b> 字段值复制到下方第二个框
      </div>
      <label for="m-basic">Client 凭证（Authorization 请求头的值）</label>
      <textarea id="m-basic" placeholder="Basic ZHJvbmVfbmRrZ2J3OmpjdGtpbXFwYXpubg=="></textarea>
      <label for="m-refresh">refresh_token</label>
      <textarea id="m-refresh" placeholder="drone_ndkgbw:daef3kdh2:efgh5678-..."></textarea>
      <div class="err-msg" id="m-err"></div>
      <div class="actions">
        <button class="btn" id="m-cancel">取消</button>
        <button class="btn btn-primary" id="m-save">保存并验证</button>
      </div>
    </div>
  </div>

  <div class="mask" id="player-modal">
    <div class="modal player-modal">
      <h2 id="p-title">视频预览</h2>
      <div class="player-info" id="p-info"></div>
      <video id="p-video" controls autoplay muted playsinline></video>
      <div class="actions">
        <button class="btn" id="p-close">关闭</button>
      </div>
    </div>
  </div>

<script>
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[c]));
}
function badge(v) {
  const cls = 'b-' + (v == null ? 'null' : v);
  return '<span class="badge ' + cls + '">' + (v == null ? 'n/a' : esc(v)) + '</span>';
}
function row(n) {
  const flv = n.flv_url || '';
  const name = n.name || '';
  const canPlay = n.api === 'ready' && !!n.flv_url;
  const nameCell = name
    ? '<span class="name-text">' + esc(name) + '</span>'
    : '<span class="name-text"><i>未命名</i></span>';
  return '<tr>' +
    '<td class="name" data-id="' + esc(n.deviceId) + '">' +
      nameCell +
      ' <button class="btn btn-rename" data-act="rename" data-id="' + esc(n.deviceId) + '" title="编辑名称">✎</button>' +
    '</td>' +
    '<td class="id">' + esc(n.deviceId) + '</td>' +
    '<td>' + badge(n.api) + '</td>' +
    '<td class="flv" title="' + esc(flv) + '">' + (flv ? esc(flv) : '—') + '</td>' +
    '<td>' + badge(n.flv_alive) + '</td>' +
    '<td>' + (n.age_s == null ? '—' : n.age_s + 's') + '</td>' +
    '<td class="actions">' +
      (canPlay ? '<button class="btn btn-play" data-act="play" data-id="' + esc(n.deviceId) + '">▶ 播放</button>' : '') +
      '<button class="btn" data-act="refresh" data-id="' + esc(n.deviceId) + '">刷新</button>' +
      '<button class="btn btn-del" data-act="del" data-id="' + esc(n.deviceId) + '">删除</button>' +
    '</td></tr>';
}
function fmtTime(ts) {
  if (!ts) return '—';
  return new Date(ts * 1000).toLocaleString('zh-CN', { hour12: false });
}
function fmtDuration(s) {
  if (!s || s <= 0) return '已过期';
  if (s < 60) return s + ' 秒';
  if (s < 3600) return Math.floor(s / 60) + ' 分钟';
  if (s < 86400) return Math.floor(s / 3600) + ' 小时';
  return Math.floor(s / 86400) + ' 天';
}
async function refreshAuth() {
  try {
    const r = await fetch('/__admin__/api/auth');
    const a = await r.json();
    const bar = document.getElementById('auth-bar');
    const state = document.getElementById('a-state');
    const meta = document.getElementById('a-meta');
    const inp = document.getElementById('device-id');
    const submitBtn = document.querySelector('#add-form button[type=submit]');
    bar.classList.remove('ok', 'warn', 'err');
    if (a.state === 'ok') {
      bar.classList.add('ok');
      state.textContent = '✓ 已认证';
      meta.innerHTML = 'token 剩余 <b>' + fmtDuration(a.expires_in_s) + '</b>，'
                    + '上次续期：' + fmtTime(a.last_refreshed_at) + '。'
                    + '到期前会自动续期。';
      inp.disabled = false; submitBtn.disabled = false;
    } else if (a.state === 'expiring') {
      bar.classList.add('warn');
      state.textContent = '⏳ 即将过期';
      meta.innerHTML = '剩余 ' + fmtDuration(a.expires_in_s) + '，下次请求会自动续期';
      inp.disabled = false; submitBtn.disabled = false;
    } else if (a.state === 'needs_bootstrap') {
      bar.classList.add('err');
      state.textContent = '⚠ 需要配置';
      meta.innerHTML = '尚未粘贴 refresh_token / Client 凭证，主动预热无法工作。'
                    + '点击右侧 <b>配置鉴权</b> 完成引导。';
      inp.disabled = true; submitBtn.disabled = true;
    } else {
      bar.classList.add('err');
      state.textContent = '✗ 失败';
      meta.innerHTML = (a.last_error ? '错误：<code>' + a.last_error + '</code>。' : '')
                    + ' refresh_token 可能已失效，请<b>重新配置鉴权</b>。';
      inp.disabled = true; submitBtn.disabled = true;
    }
  } catch (e) { console.error(e); }
}
async function refreshNests() {
  try {
    const r = await fetch('/__admin__/api/nests');
    const nests = await r.json();
    document.getElementById('s-total').textContent = nests.length;
    document.getElementById('s-ready').textContent = nests.filter(n => n.api === 'ready').length;
    document.getElementById('s-alive').textContent = nests.filter(n => n.flv_alive === 'alive').length;
    const tb = document.getElementById('tbody');
    if (!nests.length) {
      tb.innerHTML = '<tr><td colspan="7" class="empty">尚未添加任何机巢</td></tr>';
    } else {
      tb.innerHTML = nests.map(row).join('');
    }
  } catch (e) { console.error(e); }
}
async function refreshAll() { await Promise.all([refreshAuth(), refreshNests()]); }

document.getElementById('add-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const inp = document.getElementById('device-id');
  const nameInp = document.getElementById('device-name');
  const id = inp.value.trim();
  const name = nameInp.value.trim();
  if (!id) return;
  const r = await fetch('/__admin__/api/nests', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ deviceId: id, name: name })
  });
  if (!r.ok) {
    const j = await r.json().catch(() => ({}));
    alert('添加失败：' + (j.detail || r.status));
    return;
  }
  inp.value = '';
  nameInp.value = '';
  refreshNests();
});
document.getElementById('tbody').addEventListener('click', async (e) => {
  const b = e.target.closest('button[data-act]');
  if (!b) return;
  const id = b.dataset.id;
  const act = b.dataset.act;
  if (act === 'del') {
    if (!confirm('确定删除机巢 ' + id + ' 吗？这会停止保活。')) return;
    await fetch('/__admin__/api/nests/' + encodeURIComponent(id), { method: 'DELETE' });
    refreshNests();
  } else if (act === 'rename') {
    const row = b.closest('tr');
    const cur = (row.querySelector('.name-text')?.innerText || '').replace(/^未命名$/, '').trim();
    const v = prompt('设置机巢名称（留空则清除）：', cur);
    if (v === null) return;
    const r = await fetch('/__admin__/api/nests/' + encodeURIComponent(id), {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: v.trim() })
    });
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      alert('改名失败：' + (j.detail || r.status));
      return;
    }
    refreshNests();
  } else if (act === 'play') {
    const row = b.closest('tr');
    const nameText = (row.querySelector('.name-text')?.innerText || '').trim();
    openPlayer(id, nameText === '未命名' ? '' : nameText);
  } else if (act === 'refresh') {
    await fetch('/__admin__/api/nests/' + encodeURIComponent(id) + '/refresh', { method: 'POST' });
    refreshNests();
  }
});

// auth modal
const modal = document.getElementById('auth-modal');
document.getElementById('btn-auth').onclick = () => {
  document.getElementById('m-err').textContent = '';
  modal.classList.add('show');
};
document.getElementById('m-cancel').onclick = () => modal.classList.remove('show');
modal.onclick = (e) => { if (e.target === modal) modal.classList.remove('show'); };
document.getElementById('m-save').onclick = async () => {
  const basic = document.getElementById('m-basic').value.trim();
  const refresh = document.getElementById('m-refresh').value.trim();
  const err = document.getElementById('m-err');
  err.textContent = '';
  if (!basic || !refresh) { err.textContent = '两个字段都要填'; return; }
  const r = await fetch('/__admin__/api/auth', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ refresh_token: refresh, client_basic: basic })
  });
  if (!r.ok) {
    const j = await r.json().catch(() => ({}));
    err.textContent = j.detail || ('HTTP ' + r.status);
    return;
  }
  document.getElementById('m-basic').value = '';
  document.getElementById('m-refresh').value = '';
  modal.classList.remove('show');
  refreshAll();
};
document.getElementById('btn-auth-refresh').onclick = async () => {
  const r = await fetch('/__admin__/api/auth/refresh', { method: 'POST' });
  if (!r.ok) {
    const j = await r.json().catch(() => ({}));
    alert('续期失败：' + (j.detail || r.status));
  }
  refreshAuth();
};

// ── FLV 播放器（flv.js）──────────────────
let __player = null;
const playerModal = document.getElementById('player-modal');

function closePlayer() {
  if (__player) {
    try { __player.pause(); } catch(_) {}
    try { __player.unload(); } catch(_) {}
    try { __player.detachMediaElement(); } catch(_) {}
    try { __player.destroy(); } catch(_) {}
    __player = null;
  }
  const v = document.getElementById('p-video');
  if (v) { v.removeAttribute('src'); try { v.load(); } catch(_) {} }
}

function openPlayer(deviceId, name) {
  const video = document.getElementById('p-video');
  const title = document.getElementById('p-title');
  const info = document.getElementById('p-info');
  const src = '/__proxy__/flv/' + encodeURIComponent(deviceId);
  title.textContent = '▶ ' + (name ? (name + '  (' + deviceId + ')') : deviceId);
  info.textContent = '播放源：' + src;
  closePlayer();
  if (!window.flvjs || !flvjs.isSupported()) {
    info.textContent = '错误：flv.js 加载失败或浏览器不支持 MSE（请用 Chrome/Edge/Firefox）';
    playerModal.classList.add('show');
    return;
  }
  __player = flvjs.createPlayer({
    type: 'flv', url: src, isLive: true, hasAudio: false, hasVideo: true,
  }, {
    enableWorker: false, enableStashBuffer: false, stashInitialSize: 128,
    autoCleanupSourceBuffer: true,
  });
  __player.attachMediaElement(video);
  __player.load();
  __player.play().catch(err => console.warn('play error', err));
  playerModal.classList.add('show');
}

document.getElementById('p-close').onclick = () => {
  closePlayer();
  playerModal.classList.remove('show');
};
playerModal.onclick = (e) => {
  if (e.target === playerModal) {
    closePlayer();
    playerModal.classList.remove('show');
  }
};

refreshAll();
setInterval(refreshAll, 3000);
</script>
</body>
</html>
"""


def _nest_status(device_id: str) -> dict:
    entry = _cache.get(device_id)
    return {
        "deviceId": device_id,
        "name": _nest_names.get(device_id, ""),
        "api": _api_status.get(device_id, "pending"),
        "cache_ok": entry.is_valid() if entry else False,
        "age_s": round(entry.age(), 1) if entry else None,
        "flv_url": entry.flv_url if entry else None,
        "flv_alive": _flv_alive.get(entry.flv_url) if entry and entry.flv_url else None,
    }


def _auth_status() -> dict:
    """对外暴露的 token 状态（脱敏：不返回原始 token 内容，只显示前 8 位）"""
    def mask(s):
        if not s:
            return ""
        return s[:8] + "…" if len(s) > 10 else "…"
    now = int(time.time())
    expires_at = int(_auth.get("expires_at", 0) or 0)
    has_refresh = bool(_auth.get("refresh_token"))
    has_basic = bool(_auth.get("client_basic"))
    if not has_refresh or not has_basic:
        state = "needs_bootstrap"  # UI 应该提示用户粘贴
    elif _auth.get("access_token") and expires_at > now + TOKEN_REFRESH_AHEAD_SECONDS:
        state = "ok"
    elif _auth.get("access_token") and expires_at > now:
        state = "expiring"
    else:
        state = "expired_or_unknown"
    return {
        "state": state,
        "has_refresh_token": has_refresh,
        "has_client_basic": has_basic,
        "access_token_preview": mask(_auth.get("access_token", "")),
        "expires_at": expires_at,
        "expires_in_s": max(0, expires_at - now) if expires_at else 0,
        "last_refreshed_at": int(_auth.get("last_refreshed_at", 0) or 0),
        "last_error": _auth.get("last_error", "") or "",
        "auth_token_url": AUTH_TOKEN_URL,
    }


@app.get("/", response_class=HTMLResponse)
async def index():
    return INDEX_HTML


@app.get("/__admin__/api/nests")
async def admin_list_nests():
    return [_nest_status(d) for d in list(NEST_DEVICE_IDS)]


@app.post("/__admin__/api/nests")
async def admin_add_nest(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, "invalid JSON body")
    if not isinstance(payload, dict):
        raise HTTPException(400, "JSON must be an object")
    device_id = (payload.get("deviceId") or "").strip()
    name = (payload.get("name") or "").strip()
    if not device_id:
        raise HTTPException(400, "deviceId required")
    if device_id in NEST_DEVICE_IDS:
        raise HTTPException(409, "deviceId already exists")
    NEST_DEVICE_IDS.append(device_id)
    _nest_names[device_id] = name
    _persist_nests()
    asyncio.create_task(prewarm(device_id))
    return {"ok": True, "deviceId": device_id, "name": name}


@app.patch("/__admin__/api/nests/{device_id}")
async def admin_rename_nest(device_id: str, request: Request):
    """修改机巢名称"""
    if device_id not in NEST_DEVICE_IDS:
        raise HTTPException(404, "deviceId not found")
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, "invalid JSON body")
    if not isinstance(payload, dict):
        raise HTTPException(400, "JSON must be an object")
    name = (payload.get("name") or "").strip()
    _nest_names[device_id] = name
    _persist_nests()
    return {"ok": True, "deviceId": device_id, "name": name}


@app.delete("/__admin__/api/nests/{device_id}")
async def admin_delete_nest(device_id: str):
    if device_id not in NEST_DEVICE_IDS:
        raise HTTPException(404, "deviceId not found")
    NEST_DEVICE_IDS.remove(device_id)
    _nest_names.pop(device_id, None)
    _persist_nests()
    # 停掉对应的 FLV 保活任务
    entry = _cache.pop(device_id, None)
    _api_status.pop(device_id, None)
    if entry and entry.flv_url:
        stop_keepalive(entry.flv_url)
        log.info(f"🗑️ 移除机巢 [{device_id}] 并停止保活 → {entry.flv_url}")
    else:
        log.info(f"🗑️ 移除机巢 [{device_id}]")
    return {"ok": True}


@app.post("/__admin__/api/nests/{device_id}/refresh")
async def admin_refresh_nest(device_id: str):
    if device_id not in NEST_DEVICE_IDS:
        raise HTTPException(404, "deviceId not found")
    asyncio.create_task(prewarm(device_id))
    return {"ok": True}


@app.get("/__admin__/api/auth")
async def admin_auth_status():
    return _auth_status()


@app.post("/__admin__/api/auth")
async def admin_set_auth(request: Request):
    """粘贴 refresh_token + client_basic 引导 / 重置鉴权"""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, "invalid JSON body")
    if not isinstance(payload, dict):
        raise HTTPException(400, "JSON must be an object")
    refresh_token = (payload.get("refresh_token") or "").strip()
    client_basic = (payload.get("client_basic") or "").strip()
    if not refresh_token:
        raise HTTPException(400, "refresh_token required")
    if not client_basic:
        raise HTTPException(400, "client_basic required")
    _auth["refresh_token"] = refresh_token
    _auth["client_basic"] = _normalize_client_basic(client_basic)
    # 重置 access_token，强制下次请求重新换取
    _auth["access_token"] = ""
    _auth["expires_at"] = 0
    _auth["last_error"] = ""
    save_tokens()
    # 立即试一次续期，把结果返回给前端
    try:
        await refresh_access_token()
        return {"ok": True, "auth": _auth_status()}
    except Exception as e:
        raise HTTPException(400, f"refresh_token 验证失败: {e}")


@app.post("/__admin__/api/auth/refresh")
async def admin_auth_refresh():
    """手动触发一次续期，方便诊断"""
    try:
        await refresh_access_token()
        return {"ok": True, "auth": _auth_status()}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/__proxy__/status")
async def status():
    """调试接口：查看各机巢预热状态（保持向后兼容）"""
    return {d: _nest_status(d) for d in list(NEST_DEVICE_IDS)}


@app.get("/__proxy__/flv/{device_id}")
async def proxy_flv(device_id: str):
    """流式转发该机巢当前的 FLV 视频流给浏览器（同源避开 CORS）。
    浏览器端用 flv.js 加载 /__proxy__/flv/{deviceId} 即可播放。"""
    entry = _cache.get(device_id)
    if not entry or not entry.flv_url:
        raise HTTPException(404, "该机巢暂无 FLV 地址，等待预热完成或检查鉴权")
    flv_url = entry.flv_url

    async def gen():
        timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=None)
        try:
            async with session().get(
                    flv_url,
                    headers={"User-Agent": "FLV-Player/1.0"},
                    timeout=timeout,
            ) as r:
                if r.status != 200:
                    log.warning(f"[FLV-PROXY] {device_id} 上游 {r.status}")
                    return
                async for chunk in r.content.iter_chunked(8192):
                    yield chunk
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"[FLV-PROXY] {device_id} 异常: {e}")

    return StreamingResponse(
        gen(),
        media_type="video/x-flv",
        headers={"Cache-Control": "no-cache, no-store"},
    )


# ── 代理路由 ───────────────────────────────────────────

async def forward(request: Request) -> Response:
    body = await request.body()
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    try:
        async with session().request(
                request.method,
                UPSTREAM + str(request.url.path),
                params=dict(request.query_params),
                headers=headers, data=body,
                timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            rb = await resp.read()
            return Response(rb, resp.status, dict(resp.headers))
    except Exception as e:
        return JSONResponse({"error": "upstream error", "detail": str(e)}, 502)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(request: Request, path: str):
    # 非 startLive 接口 → 直接透明转发
    if STREAM_API_PATH not in request.url.path:
        return await forward(request)

    # 提取 deviceId
    device_id = None
    try:
        raw = await request.body()
        device_id = json.loads(raw).get("deviceId") if raw else None
    except Exception:
        pass

    if not device_id:
        return await forward(request)

    entry = _cache.get(device_id)

    if entry and entry.is_valid():
        # ⚡ 缓存命中
        log.info(f"⚡ 命中缓存 [{device_id[-8:]}...] age={entry.age():.1f}s flv={entry.flv_url}")
        skip = {"transfer-encoding", "content-encoding", "content-length"}
        headers = {k: v for k, v in entry.headers.items() if k.lower() not in skip}
        if entry.age() > CACHE_TTL * 0.5:
            asyncio.create_task(prewarm(device_id))  # 提前后台刷新
        return Response(entry.body, entry.status_code, headers)

    # ⏳ 缓存未命中（首次启动窗口期）→ 实时请求
    log.warning(f"⏳ 缓存未命中 [{device_id[-8:]}...]，实时请求中...")
    t0 = time.time()
    old_flv_url = entry.flv_url if entry else None
    resp = await forward(request)
    log.info(f"实时耗时: {(time.time() - t0) * 1000:.0f}ms")
    if resp.status_code == 200:
        flv_url = extract_flv(resp.body)
        _cache[device_id] = CacheEntry(
            resp.status_code, dict(resp.headers), resp.body, flv_url
        )
        _api_status[device_id] = "ready"
        if flv_url:
            if old_flv_url and old_flv_url != flv_url:
                stop_keepalive(old_flv_url)
            start_keepalive(flv_url)
    return resp


if __name__ == "__main__":
    log.info("=" * 56)
    log.info(f"  上游服务  : {UPSTREAM}")
    log.info(f"  代理端口  : {PROXY_PORT}  →  Java 改用此端口")
    log.info(f"  拦截接口  : {STREAM_API_PATH}")
    log.info(f"  持久化    : {NESTS_FILE}")
    log.info(f"  管理 UI   : http://<本机IP>:{PROXY_PORT}/")
    log.info(f"  缓存TTL   : {CACHE_TTL}s")
    log.info("=" * 56)
    uvicorn.run(app, host="0.0.0.0", port=PROXY_PORT, log_level="warning")
