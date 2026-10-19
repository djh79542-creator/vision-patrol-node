#!/usr/bin/env bash
# 本地/内网构建入口：编译镜像并按需推送到私有 registry。
# 用法：
#   scripts/build-image.sh build            # 本机架构，打 branch 标签
#   scripts/build-image.sh build-amd64      # linux/amd64，服务器部署用
#   scripts/build-image.sh push             # build-amd64 + tag + push
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REGISTRY="${REGISTRY:-registry.internal/patrol}"
IMAGE_NAME="${IMAGE_NAME:-vision-patrol-node}"
LOCAL_TAG="${LOCAL_TAG:-$IMAGE_NAME}"

# 分支名直接当镜像标签，斜杠换成连字符；非 git 目录退回 latest
branch="$(git -C "$ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
if [[ -z "$branch" ]]; then
  IMAGE_TAG="latest"
else
  IMAGE_TAG="${branch//[\/:\\]/-}"
fi
FULL_IMAGE="$REGISTRY/$IMAGE_NAME:$IMAGE_TAG"

command="${1:-}"

build_args=(
  -f docker/Dockerfile
  --build-arg BUILDKIT_INLINE_CACHE=1
)

require_docker() {
  command -v docker >/dev/null || { echo "找不到 docker 命令" >&2; exit 1; }
  docker info >/dev/null 2>&1 || { echo "docker 守护进程未响应" >&2; exit 1; }
}

case "$command" in
  build)
    require_docker
    echo "==> 本机架构构建 $LOCAL_TAG:$IMAGE_TAG"
    docker build "${build_args[@]}" -t "$LOCAL_TAG:$IMAGE_TAG" .
    ;;
  build-amd64)
    require_docker
    echo "==> linux/amd64 构建 $LOCAL_TAG:$IMAGE_TAG"
    docker build --platform linux/amd64 --load --network=host \
      --build-arg APT_USE_ALIYUN_MIRROR=1 \
      --build-arg PIP_INDEX_URL="${PIP_INDEX_URL:-https://pypi.org/simple}" \
      --build-arg PIP_TRUSTED_HOST="${PIP_TRUSTED_HOST:-}" \
      "${build_args[@]}" -t "$LOCAL_TAG:$IMAGE_TAG" .
    ;;
  push)
    require_docker
    docker image inspect "$LOCAL_TAG:$IMAGE_TAG" >/dev/null 2>&1 || {
      echo "本地没有 $LOCAL_TAG:$IMAGE_TAG，先执行 build 或 build-amd64" >&2; exit 1; }
    echo "==> 推送 $FULL_IMAGE"
    docker tag "$LOCAL_TAG:$IMAGE_TAG" "$FULL_IMAGE"
    docker push "$FULL_IMAGE"
    ;;
  up)
    require_docker
    docker compose -f docker/docker-compose.yml up -d
    echo "==> 已启动，日志：scripts/build-image.sh logs"
    ;;
  down)
    docker compose -f docker/docker-compose.yml down
    ;;
  logs)
    docker compose -f docker/docker-compose.yml logs -f patrol-node
    ;;
  clean)
    require_docker
    container="$(docker compose -f docker/docker-compose.yml ps -q 2>/dev/null || true)"
    [[ -n "$container" ]] && docker rm -f $container
    if [[ "${2:-}" == "--images" ]]; then
      docker rmi "$LOCAL_TAG:$IMAGE_TAG" 2>/dev/null || echo "本地无镜像可清理"
    fi
    ;;
  *)
    cat <<EOF
用法: scripts/build-image.sh {build|build-amd64|push|up|down|logs|clean [--images]}

  REGISTRY=$REGISTRY  IMAGE_NAME=$IMAGE_NAME  IMAGE_TAG=$IMAGE_TAG
EOF
    exit 1
    ;;
esac
