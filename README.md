# DJI 机巢视频流预热代理

> 解决 Java 调用 DJI `startLive` 接口的 3 秒首帧延迟问题。  
> **Java 代码零改动**，只需把上游地址端口改成本代理。

---

## 一、原理

```
原方案：  Java ──► startLive(3s 等待) ──► FLV 地址 ──► 拉流播放
本代理：  Java ──► 本代理(命中缓存, <50ms) ──► FLV 地址 ──► 拉流播放
                  ▲
                  └── 后台预调 + FLV 保活，让流服务器始终在线
```

1. 启动时**并发预调**所有机巢的 `startLive` 接口，缓存返回的 FLV 地址  
2. 同时对每条 FLV 建立**保活连接**，让流服务器推流进程持续在线  
3. Java 点播时代理**直接命中缓存**返回，无需等待  
4. 后台周期性刷新缓存（默认 30s 检查、90s TTL）

---

## 二、文件结构

```
rtsp/
├── stream_proxy.py        # 代理主程序（挂载进容器，改配置不用重建镜像）
├── requirements.txt       # Python 依赖锁定版本
├── Dockerfile             # 镜像构建配置（python:3.10-slim + 清华源）
├── docker-compose.yml     # 容器编排（仅供参考，本 README 不依赖它）
├── .dockerignore          # 构建排除清单
├── build-and-export.sh    # 一键构建 + 导出镜像 + 打成 deploy 包
└── README.md              # 本文件
```

> ⚠️ **命令约定**：本项目所有运维命令默认使用 `docker` 原生命令，**不依赖 `docker compose` 插件**。  
> 如果你的服务器装了 Compose V2 插件，也可以用 `docker compose up -d` 等命令替代，效果等价。

---

## 三、完整打包 → 部署流程

整个流程分三步:**开发机构建** → **传输** → **内网启动**。

### 步骤 1:开发机构建并导出镜像（需联网）

前置:开发机装好 Docker Desktop（或 Docker Engine + buildx）。

```bash
cd /Users/ranyong/Desktop/code/rtsp

# 给脚本加执行权限（首次执行）
chmod +x build-and-export.sh

# 一键构建 + 导出
./build-and-export.sh
```

执行流程：

1. `docker buildx build --platform linux/amd64` 构建镜像（默认 x86_64，适配普通 Linux 服务器）  
2. `docker save` 把镜像导出成 `dji-stream-proxy-latest.tar`（约 150 MB）
3. **`tar czf` 把镜像 + docker-compose.yml + stream_proxy.py 一起打成 `dji-proxy-deploy.tar.gz`**（约 50 MB，压缩后）

> **架构注意**：脚本默认 `linux/amd64`。如果内网服务器是 ARM（如鲲鹏、飞腾），  
> 编辑 `build-and-export.sh`，把 `PLATFORM=linux/amd64` 改成 `linux/arm64` 再执行。

> **构建慢/拉不到基础镜像**：  
> `Dockerfile` 默认已经用 `docker.m.daocloud.io/library/python:3.10-slim` 走 DaoCloud 镜像源，  
> 国内网络可直接构建，无需额外配置。  
> 如果 DaoCloud 也访问不了，可改成其他镜像源，例如：
> - `docker.1ms.run/library/python:3.10-slim`
> - `dockerproxy.com/library/python:3.10-slim`
>
> 或者在 Docker Desktop → Settings → Docker Engine 里配 `registry-mirrors`，
> 然后把 Dockerfile 第一行改回 `FROM python:3.10-slim`。

完成后，当前目录会生成两个文件：

```
dji-stream-proxy-latest.tar    ← 镜像包（已被 deploy 包含，可不传）
dji-proxy-deploy.tar.gz        ← 部署包，只传这一个到内网即可 ⭐
```

### 步骤 2：把部署包传到内网服务器

只需要传 **一个文件**：`dji-proxy-deploy.tar.gz`（约 50 MB，里面已包含镜像 + 编排 + 业务代码）。

传输方式按现场环境选：

```bash
# 方式 A：scp（有跳板机/SSH 可达）
scp dji-proxy-deploy.tar.gz user@内网IP:/opt/dji-proxy/

# 方式 B：U 盘/移动硬盘
# 把 dji-proxy-deploy.tar.gz 拷到 U 盘，插到内网服务器后 cp 到 /opt/dji-proxy/

# 方式 C：内网文件服务器
# 上传到 ftp/nas/堡垒机文件柜，内网机器再下载
```

### 步骤 3：内网服务器解压、加载并启动

```bash
# 进入工作目录（首次部署需先创建）
mkdir -p /opt/dji-proxy && cd /opt/dji-proxy

# 1. 解压部署包，会得到 3 个文件：
#    - dji-stream-proxy-latest.tar  (镜像)
#    - docker-compose.yml           (编排配置，可不用)
#    - stream_proxy.py              (业务代码，会被挂载进容器)
tar xzf dji-proxy-deploy.tar.gz

# 2. 加载镜像
docker load -i dji-stream-proxy-latest.tar
# 输出: Loaded image: dji-stream-proxy:latest

# 3. 验证镜像已加载
docker images | grep dji-stream-proxy

# 4. 编辑配置（首次必做）
vim stream_proxy.py
#   ── 改 UPSTREAM       = Java 后端地址
#   ── 改 NEST_DEVICE_IDS = 全部机巢的 deviceId 列表

# 5. 启动容器（docker 原生命令，不需要 compose 插件）
docker run -d \
  --name dji-stream-proxy \
  --restart unless-stopped \
  --network host \
  -v "$(pwd)/stream_proxy.py:/app/stream_proxy.py:ro" \
  --log-driver json-file \
  --log-opt max-size=10m \
  --log-opt max-file=3 \
  dji-stream-proxy:latest

# 6. 看日志确认预热成功
docker logs -f dji-stream-proxy
```

预期日志：

```
🔥 并发预热 15 个机巢...
✅ 预热完成 [...A2KBG] → http://x.x.x.x/live/xxx.flv
✅ 预热完成 [...] → ...
预热完成：15/15 成功
[FLV] 保活建立 → http://...
── 状态 接口缓存:15/15  FLV保活:15/15
```

看到 `接口缓存:15/15  FLV保活:15/15` 就说明 OK。

> 💡 **如果服务器装了 Compose V2 插件**：可以用 `docker compose up -d` 一条命令替代上面的 `docker run`，配置直接从同目录的 `docker-compose.yml` 读取。

### 步骤 4：切换 Java 配置

修改 Java/前端配置文件，把上游地址端口改成本代理：

```diff
- http://172.29.0.14:8888/api/proxy/djVideo/startLive
+ http://<内网服务器IP>:9000/api/proxy/djVideo/startLive
```

代理对所有路径透明转发，只对 `startLive` 接口做缓存。

---

## 四、配置说明

所有配置在 `stream_proxy.py` 顶部的【配置区】：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `UPSTREAM` | `http://172.29.0.14:8888` | Java 后端原始地址，**必改** |
| `PROXY_PORT` | `9000` | 代理监听端口 |
| `NEST_DEVICE_IDS` | `[...]` | 全部机巢的 deviceId 列表，**必改** |
| `CACHE_TTL` | `90` | 缓存有效期（秒） |
| `FLV_CHUNKS_PER_SECOND` | `2` | 保活带宽节流（每路约 8KB/s） |
| `FLV_CHUNK_SIZE` | `4096` | 保活每次读取字节数 |

**改完配置后**重启容器即可（`stream_proxy.py` 是挂载进容器的，**不用重建镜像**）：

```bash
docker restart dji-stream-proxy
```

---

## 五、日常运维

```bash
# 查看运行状态
docker ps | grep dji-stream-proxy

# 实时日志
docker logs -f dji-stream-proxy

# 只看最近 100 行
docker logs --tail=100 dji-stream-proxy

# 健康检查（看每个机巢的缓存/保活状态）
curl http://localhost:9000/__proxy__/status | jq .

# 重启（改了 stream_proxy.py 后）
docker restart dji-stream-proxy

# 停止并删除容器
docker stop dji-stream-proxy && docker rm dji-stream-proxy

# 再次启动：复制"步骤 3 第 5 步"的 docker run 命令重跑即可
```

### 升级业务代码

只改 `stream_proxy.py`：

```bash
vim stream_proxy.py
docker restart dji-stream-proxy       # 不需要重新构建镜像
```

### 升级依赖（改了 requirements.txt）

需要回开发机重新构建打包：

```bash
# === 开发机 ===
./build-and-export.sh
# 把新的 dji-proxy-deploy.tar.gz 传到内网

# === 内网服务器 ===
cd /opt/dji-proxy

# 1. 停止并删除旧容器
docker stop dji-stream-proxy && docker rm dji-stream-proxy

# 2. 解压新包（会覆盖原文件）
tar xzf dji-proxy-deploy.tar.gz

# 3. 加载新镜像（同 tag 会自动覆盖旧的）
docker load -i dji-stream-proxy-latest.tar

# 4. 重新启动（复用步骤 3 第 5 步的 docker run 命令）
docker run -d \
  --name dji-stream-proxy \
  --restart unless-stopped \
  --network host \
  -v "$(pwd)/stream_proxy.py:/app/stream_proxy.py:ro" \
  --log-driver json-file \
  --log-opt max-size=10m \
  --log-opt max-file=3 \
  dji-stream-proxy:latest
```

---

## 六、故障排查

### Q1：Mac/Windows 本地调试报 `network_mode: host is not supported`

`--network host` 只在 Linux 上有效。本地调试时改用端口映射：

```bash
docker run -d \
  --name dji-stream-proxy \
  --restart unless-stopped \
  -p 9000:9000 \
  -v "$(pwd)/stream_proxy.py:/app/stream_proxy.py:ro" \
  dji-stream-proxy:latest
```

注意：端口映射模式下，FLV 保活如果连的是内网地址，可能不通，这是正常的。

### Q2：日志里 `❌ 预热失败` 或 `❌ FLV 408/504`

- 检查 `UPSTREAM` 地址是否能从容器宿主机 ping 通  
- 检查 `NEST_DEVICE_IDS` 里的 ID 是否正确  
- 在宿主机 `curl -X POST http://172.29.0.14:8888/api/proxy/djVideo/startLive -H "Content-Type: application/json" -d '{"deviceId":"xxx","videoType":1}'` 手动验证

### Q3：Java 调用返回 502

代理跟上游通信失败。看代理日志：

```bash
docker logs --tail=200 dji-stream-proxy | grep -i upstream
```

### Q4：内网服务器架构不是 x86_64

报错形如 `exec format error`。  
回开发机改 `build-and-export.sh` 里的 `PLATFORM=linux/arm64`，重新构建导出。

### Q5：想看缓存命中率

代理日志里搜索：

```bash
docker logs dji-stream-proxy | grep -c "⚡ 命中缓存"     # 命中次数
docker logs dji-stream-proxy | grep -c "⏳ 缓存未命中"    # 未命中次数
```

### Q6：执行 `docker compose up -d` 报 `unknown shorthand flag: 'd' in -d`

说明服务器没装 Compose V2 插件，`docker compose` 子命令不可用。  
**本 README 全部使用 `docker` 原生命令，不需要 compose**，按"步骤 3 第 5 步"的 `docker run` 启动即可。

如果你坚持要用 compose，按下面安装 V2 插件：
```bash
# Ubuntu/Debian
sudo apt-get install -y docker-compose-plugin

# CentOS/RHEL/Rocky
sudo yum install -y docker-compose-plugin

# 离线服务器（直接下二进制）
mkdir -p ~/.docker/cli-plugins
curl -SL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 \
  -o ~/.docker/cli-plugins/docker-compose
chmod +x ~/.docker/cli-plugins/docker-compose
docker compose version    # 验证
```

---

## 七、附录：手动构建（不用脚本）

如果 `build-and-export.sh` 跑不通，等价的手动命令：

```bash
# === 开发机 ===

# 1. 构建（指定平台 + 加载到本地 docker）
docker buildx build --platform linux/amd64 -t dji-stream-proxy:latest --load .

# 2. 导出镜像
docker save dji-stream-proxy:latest -o dji-stream-proxy-latest.tar

# 3. 打成部署包（镜像 + 编排 + 业务代码）
tar czf dji-proxy-deploy.tar.gz \
    dji-stream-proxy-latest.tar \
    docker-compose.yml \
    stream_proxy.py

# === 内网服务器 ===

# 4. 解压
tar xzf dji-proxy-deploy.tar.gz

# 5. 加载镜像
docker load -i dji-stream-proxy-latest.tar

# 6. 启动（docker 原生命令）
docker run -d \
  --name dji-stream-proxy \
  --restart unless-stopped \
  --network host \
  -v "$(pwd)/stream_proxy.py:/app/stream_proxy.py:ro" \
  --log-driver json-file \
  --log-opt max-size=10m \
  --log-opt max-file=3 \
  dji-stream-proxy:latest
```
