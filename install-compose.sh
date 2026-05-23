#!/usr/bin/env bash
# 在内网服务器上离线安装 docker compose v2（作为 docker CLI 插件）
# 同时安装一份独立 docker-compose 命令兼容旧版用法
set -euo pipefail

BIN="docker-compose-linux-x86_64"

if [ ! -f "$BIN" ]; then
  echo "❌ 当前目录找不到 $BIN，请确认是否已解压部署包"
  exit 1
fi

# 决定插件目录（优先官方推荐路径，其次发行版常见路径）
if [ -d /usr/local/lib/docker ]; then
  PLUGIN_DIR=/usr/local/lib/docker/cli-plugins
elif [ -d /usr/libexec/docker ]; then
  PLUGIN_DIR=/usr/libexec/docker/cli-plugins
else
  PLUGIN_DIR=/usr/local/lib/docker/cli-plugins
fi

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  SUDO="sudo"
fi

echo ">> 安装为 docker CLI 插件: $PLUGIN_DIR/docker-compose"
$SUDO mkdir -p "$PLUGIN_DIR"
$SUDO cp "$BIN" "$PLUGIN_DIR/docker-compose"
$SUDO chmod +x "$PLUGIN_DIR/docker-compose"

echo ">> 同时安装独立命令: /usr/local/bin/docker-compose"
$SUDO cp "$BIN" /usr/local/bin/docker-compose
$SUDO chmod +x /usr/local/bin/docker-compose

echo ""
echo "验证安装结果:"
echo "----------------------------"
docker compose version || { echo "❌ docker compose 子命令仍然不可用"; exit 1; }
echo ""
docker-compose --version || true
echo "----------------------------"
echo ""
echo "✅ 完成。现在可以用 docker compose up -d 启动代理了。"
