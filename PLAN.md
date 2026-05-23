# DJI 机巢视频流预热代理 — 开发计划

## 项目背景

Java 系统拉取 Python 管理的 DJI 无人机机巢视频流，每次点播需等待约 3 秒。
**不能改 Java 代码**，目标是用一个独立脚本实现超低延迟点播。

---

## 已确认的技术细节（来自抓包）

### 点播流程
```
① Java/前端 POST startLive  →  拿到 FLV 地址
② 用 FLV 地址直接拉流播放
```

### startLive 接口
| 项目 | 值 |
|------|-----|
| 地址 | `http://172.29.0.14:8888/api/proxy/djVideo/startLive` |
| 方法 | POST |
| Content-Type | application/json |
| 请求 Body | `{"deviceId": "182298869415582xxxx", "videoType": 1}` |
| 响应结构 | `{"code": 0, "data": "http://172.29.0.13:8080/dji/7CTDMxxxx.165-0-7.flv", "success": true}` |
| FLV地址字段 | `data` |
| 机巢标识字段 | `deviceId` |

### 流媒体服务
| 项目 | 值 |
|------|-----|
| 流服务器 | `http://172.29.0.13:8080` |
| FLV 路径格式 | `/dji/{序列号}.165-0-7.flv` |
| 延迟来源 | startLive 触发后端初始化推流进程（约3秒） |

---

## 已完成：stream_proxy.py

**文件位置**：`stream_proxy.py`（同目录）

### 实现原理
```
脚本启动
  ├─ 并发调用所有机巢 startLive → 缓存响应（含FLV地址）   解决接口3秒
  └─ 对每个 FLV 地址建立保活长连接，持续拉流丢弃          解决首帧延迟

Java 点播
  └─ 打代理端口 → 命中缓存 <10ms → FLV流已在线 → 秒出画面
```

### 核心模块
- `prewarm(device_id)` — 异步调用 startLive 并缓存结果
- `flv_worker(flv_url)` — 独立线程持续拉 FLV 流保活
- `bg_refresh()` — 后台定时刷新过期缓存
- `proxy()` — FastAPI 路由，命中缓存直接返回，否则透明转发
- `GET /__proxy__/status` — 调试接口，查看各机巢预热状态

### 依赖
```bash
pip install fastapi uvicorn aiohttp requests
```

### 启动
```bash
python stream_proxy.py
```

---

## 待完成 / 待验证

### 🔴 必须完成

- [ ] **填入全部15个机巢的 deviceId**
  - 方法：对每个机巢点一次点播，从 DevTools → startLive → 载荷里复制 deviceId
  - 填入 `stream_proxy.py` 配置区的 `NEST_DEVICE_IDS` 列表

- [ ] **修改前端/Java 配置**，把 startLive 地址端口改为代理端口 9000
  ```
  原：http://172.29.0.14:8888/api/proxy/djVideo/startLive
  改：http://<代理机器IP>:9000/api/proxy/djVideo/startLive
  ```

- [ ] **验证预热是否成功**
  - 访问 `http://127.0.0.1:9000/__proxy__/status`
  - 每个机巢应显示 `"api": "ready"` 且 `"flv_alive": "alive"`

### 🟡 需要确认

- [ ] `flv_alive` 是否能成功为 `alive`
  - 如果为 `null`，说明 FLV 地址提取失败，需要检查响应 JSON 结构
  - 如果为 `dead`，说明流服务器 172.29.0.13:8080 拒绝了保活连接

- [ ] 实际点播延迟测试（对比前后）
  - 改配置前记录一次点播耗时
  - 接代理后再记录，预期从 3000ms → <100ms

- [ ] 代理脚本部署位置确认
  - 需要和 Java 服务在同一内网（能访问 172.29.0.14 和 172.29.0.13）
  - 建议部署在 Java 服务同一台机器上，直接用 127.0.0.1:9000

### 🟢 后续优化（可选）

- [ ] 用 `supervisor` 或 `systemd` 把脚本注册为系统服务，开机自启
- [ ] 把 deviceId 列表改为从配置文件或数据库读取，方便动态增减机巢
- [ ] `/__proxy__/status` 接口加简单鉴权，防止暴露在公网
- [ ] 监控告警：当某机巢 FLV 保活持续失败超过 N 次时发通知

---

## 文件结构

```
rtsp/
├── stream_proxy.py    ← 代理主程序（已完成）
└── PLAN.md            ← 本文件
```

---

## 快速参考

```bash
# 启动代理
python stream_proxy.py

# 查看预热状态
curl http://127.0.0.1:9000/__proxy__/status | python -m json.tool

# 手动测试点播接口（替换 deviceId）
curl -X POST http://127.0.0.1:9000/api/proxy/djVideo/startLive \
  -H "Content-Type: application/json" \
  -d '{"deviceId": "182298869415582xxxx", "videoType": 1}'
```
