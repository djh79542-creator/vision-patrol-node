# 容器化部署

## 命名

Compose 服务名 `patrol-node`，容器名 `patrol-node-1`；`docker compose logs` 与 `docker ps` 看到的名字一致。

要跑多个实例，复制一份服务块，同时改掉这几处，否则同组内成员名相撞会导致消息认领混乱：

- 服务键与 `container_name`
- `ports` 左侧宿主机端口
- `volumes` 路径（每个实例独立的 models / logs 目录）
- `STREAM_MEMBER`

---

## 数据卷

宿主机路径直接写在 `docker/docker-compose.yml` 里，不绕 `.env` 变量：

```yaml
volumes:
  - ./volumes/patrol-node-1/models:/app/models
  - ./volumes/patrol-node-1/logs:/app/logs
```

相对 `docker/` 目录解析。模型权重不打进镜像（`.dockerignore` 已排除 `models`），首次缺失时由 `BUNDLE_DIR` 逻辑自动拉取；`logs` 需要 `.env` 里设 `LOG_DIR=/app/logs` 才会落盘。

服务器上的实际路径若不同，直接改这两行即可。

`nginx.conf` 同理：构建时 `COPY` 进镜像，运行时不挂卷覆盖。

---

## 一个容器里跑两样东西

`CMD` 是 `nginx && exec python -u run.py`——nginx 只负责暴露探活端口，真正消费队列的是前台 Python 进程（`exec` 让 SIGTERM 直达它）。

- `ports: "9090:9090"`：宿主机 9090 被占用时改左侧，如 `8836:9090`，右侧要和 `docker/nginx.conf` 的 `listen` 保持一致。

---

## 文件一览

```
docker/
├── Dockerfile              pip 依赖 + 权重目录留空 + COPY 进 nginx.conf
├── docker-compose.yml      只拉镜像、起容器，不负责构建
├── nginx.conf              9090 探活
├── volumes/patrol-node-1/  models + logs
├── .env.example
└── README.md
```

---

## 上手

```bash
cp docker/.env.example docker/.env
# 改 docker/.env：队列地址、账本库、下游工作流地址
# 镜像地址由 REGISTRY / IMAGE_NAME / IMAGE_TAG 三段拼出，换环境只动这三行

docker compose -f docker/docker-compose.yml up -d
```

Compose 不做构建。构建与推送在仓库根目录：

```bash
bash scripts/build-image.sh build-amd64   # linux/amd64
bash scripts/build-image.sh push
# 或者 make build-amd64 / make push
```

国内网络构建慢：`--build-arg APT_USE_ALIYUN_MIRROR=1`，pip 源用 `PIP_INDEX_URL` / `PIP_TRUSTED_HOST` 两个 build-arg 覆盖。

---

## 常用命令

```bash
make up | down | logs | clean
bash scripts/build-image.sh clean --images   # 连带删掉本地镜像
```
