# Docker 部署说明

## 文件清单

```
rtsp/
├── stream_proxy.py        # 代理主程序（运行时挂载，改配置直接编辑这个文件）
├── requirements.txt       # Python 依赖
├── Dockerfile             # 镜像构建文件
├── docker-compose.yml     # 容器编排
├── .dockerignore
├── build-and-export.sh    # 一键构建 + 导出 tar 包
└── DEPLOY.md              # 本文件
```

## 一、在能联网的开发机上构建镜像

```bash
# 给脚本加执行权限
chmod +x build-and-export.sh

# 一键构建并导出（默认平台 linux/amd64，适配 x86_64 内网服务器）
./build-and-export.sh
```

执行完会生成 `dji-stream-proxy-latest.tar`（约 100~150MB）。

> 如果内网服务器是 ARM 架构，编辑脚本里的 `PLATFORM=linux/arm64` 再执行。

## 二、把文件拷到内网服务器

需要拷过去的文件：

- `dji-stream-proxy-latest.tar` — 镜像包
- `docker-compose.yml` — 启动配置
- `stream_proxy.py` — 业务代码（挂载进容器，改配置不用重建镜像）

```bash
# 示例：通过跳板机 scp
scp dji-stream-proxy-latest.tar docker-compose.yml stream_proxy.py user@内网IP:/opt/dji-proxy/
```

> `./data/` 目录不需要从开发机拷过去 —— docker compose 启动时会自动创建（用于持久化机巢列表）。

## 三、在内网服务器上启动

```bash
cd /opt/dji-proxy

# 1. 【可选】如果服务器没装 docker compose 插件，先离线装一下
#    docker compose version  能输出版本号就跳过这一步
sudo bash install-compose.sh

# 2. 导入镜像
docker load -i dji-stream-proxy-latest.tar

# 3. 启动容器
docker compose up -d

# 4. 查看日志确认预热成功
docker compose logs -f
```

> 怎么判断要不要装 compose？运行 `docker compose version`：
> - 输出版本号 → 跳过，已经有了
> - 报 "docker: 'compose' is not a docker command" → 跑 `install-compose.sh`
>
> 安装脚本会同时装两份：`docker compose`（推荐用法）+ `docker-compose`（兼容旧用法）。

预期看到类似日志：

```
📂 从 /app/data/nests.json 加载机巢 0 个
🔥 当前没有机巢，跳过预热（请通过 UI 添加）
```

首次启动列表为空 —— 浏览器打开 `http://<服务器IP>:9000/` 用管理 UI 添加机巢即可。
添加后会立即看到：

```
🔥 并发预热 N 个机巢...
✅ 预热完成 [...] → http://.../xxx.flv
── 状态 接口缓存:N/N  FLV保活:N/N
```

## 四、管理机巢（最常见的操作）

**全部通过浏览器 UI 完成**，不需要再编辑代码或重启容器：

```
http://<服务器IP>:9000/
```

页面功能：
- 输入 deviceId → 点添加 → 立即开始预热和 FLV 保活
- 表格显示每台机巢的接口状态、FLV 地址、保活状态、缓存年龄
- 删除按钮：移除机巢并停止其保活线程
- 刷新按钮：强制重新调用 startLive 接口

机巢列表持久化在 `./data/nests.json`，容器重启不丢。可以直接 `cat`/`vim` 这个文件来手动备份或批量编辑（编辑后重启容器生效）。

## 五、改其他配置

编辑当前目录的 `stream_proxy.py`：

- `UPSTREAM = "http://172.29.0.14:8888"` — Java 后端地址
- `PROXY_PORT = 9000` — 代理监听端口
- `CACHE_TTL = 90` — 缓存有效期（秒）

改完重启即可（不需要重新构建镜像，因为 py 文件是挂载进去的）：

```bash
docker compose restart
```

## 六、其他常用命令

```bash
# 查看运行状态
docker compose ps

# 实时日志
docker compose logs -f

# 查看预热状态（健康检查）
curl http://localhost:9000/__proxy__/status

# 停止
docker compose down

# 升级业务代码（py 改完后）
docker compose restart

# 升级依赖（requirements.txt 改完后，需要重新构建镜像）
#   1. 开发机重新执行 ./build-and-export.sh
#   2. 内网服务器：
docker compose down
docker load -i dji-stream-proxy-latest.tar
docker compose up -d
```

## 七、网络说明

`docker-compose.yml` 使用 `network_mode: host`，原因：

1. 代理要去内网 `172.29.0.14:8888` 拉取数据，直接用宿主机网络更简单
2. FLV 保活连接的也是内网流媒体地址，host 网络避免 NAT 问题
3. 端口 9000 直接监听在宿主机上，Java 改用 `http://<服务器IP>:9000` 即可

> host 模式只在 Linux 生效。Mac/Windows 上调试时把 `network_mode: host` 注释掉，
> 改成 `ports: ["9000:9000"]`，但 FLV 保活可能无法连上内网地址。
