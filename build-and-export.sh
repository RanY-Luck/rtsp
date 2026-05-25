#!/usr/bin/env bash
# 本地构建镜像并打包成一个部署包，用于内网离线部署
set -euo pipefail

IMAGE_NAME="dji-stream-proxy"
IMAGE_TAG="latest"
PLATFORM="linux/amd64"  # 内网服务器一般是 x86_64；如果是 ARM 服务器改成 linux/arm64
IMAGE_TAR="${IMAGE_NAME}-${IMAGE_TAG}.tar"
DEPLOY_BUNDLE="dji-proxy-deploy.tar.gz"

echo ">> 构建镜像 ${IMAGE_NAME}:${IMAGE_TAG} (platform=${PLATFORM})"
docker buildx build \
    --platform "${PLATFORM}" \
    -t "${IMAGE_NAME}:${IMAGE_TAG}" \
    --load \
    .

echo ">> 导出镜像到 ${IMAGE_TAR}"
docker save "${IMAGE_NAME}:${IMAGE_TAG}" -o "${IMAGE_TAR}"

# 准备 compose 离线安装组件（如果存在的话）
COMPOSE_FILES=()
if [ -f "bag/docker-compose-linux-x86_64" ]; then
    cp bag/docker-compose-linux-x86_64 ./docker-compose-linux-x86_64
    COMPOSE_FILES+=("docker-compose-linux-x86_64" "install-compose.sh")
    echo ">> 检测到 bag/docker-compose-linux-x86_64，将一并打包"
fi

echo ">> 打包部署文件到 ${DEPLOY_BUNDLE}"
tar czf "${DEPLOY_BUNDLE}" \
    "${IMAGE_TAR}" \
    docker-compose.yml \
    stream_proxy.py \
    ${COMPOSE_FILES[@]+"${COMPOSE_FILES[@]}"}

# 清理临时拷贝
[ -f docker-compose-linux-x86_64 ] && rm -f docker-compose-linux-x86_64

IMAGE_SIZE=$(du -h "${IMAGE_TAR}" | cut -f1)
BUNDLE_SIZE=$(du -h "${DEPLOY_BUNDLE}" | cut -f1)

echo ""
echo "✅ 完成！"
echo "   镜像文件 : ${IMAGE_TAR}      (${IMAGE_SIZE})"
echo "   部署打包 : ${DEPLOY_BUNDLE}  (${BUNDLE_SIZE})   ← 把这一个传到内网即可"
echo ""
echo "内网服务器操作："
echo "   1. 把 ${DEPLOY_BUNDLE} 拷到目标目录，例如 /opt/dji-proxy/"
echo "   2. cd /opt/dji-proxy && tar xzf ${DEPLOY_BUNDLE}"
echo "   3. 【可选】docker compose 没装的话：sudo bash install-compose.sh"
echo "   4. docker load -i ${IMAGE_TAR}"
echo "   5. ── 推荐 ──   docker compose up -d"
echo "                  docker compose logs -f"
echo "      ── 备选 ──   mkdir -p data && docker run -d \\"
echo "        --name dji-stream-proxy \\"
echo "        --restart unless-stopped \\"
echo "        --network host \\"
echo "        -v \"\$(pwd)/stream_proxy.py:/app/stream_proxy.py:ro\" \\"
echo "        -v \"\$(pwd)/data:/app/data\" \\"
echo "        --log-driver json-file --log-opt max-size=10m --log-opt max-file=3 \\"
echo "        ${IMAGE_NAME}:${IMAGE_TAG}"
echo "   6. 浏览器访问 http://<服务器IP>:9000/  → 点【配置鉴权】粘 token → 添加机巢"
