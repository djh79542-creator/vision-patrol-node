FROM python:3.10-slim-bookworm

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive

# 内网构建时可打开 apt 镜像开关并指定 pip 源
ARG APT_USE_ALIYUN_MIRROR=0
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=
RUN if [ "$APT_USE_ALIYUN_MIRROR" = "1" ]; then \
        find /etc/apt -type f \( -name "*.sources" -o -name "*.list" \) -exec sed -i \
            "s/deb.debian.org/mirrors.aliyun.com/g;s/security.debian.org/mirrors.aliyun.com/g" {} +; \
    fi \
    && apt-get update \
    && apt-get install -y --no-install-recommends nginx libglib2.0-0 libgl1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -i "$PIP_INDEX_URL" --trusted-host "$PIP_TRUSTED_HOST"

COPY . .

# 同容器内跑一份 nginx 只为暴露探活端口，主进程仍是消费者
COPY docker/nginx.conf /etc/nginx/nginx.conf

ENV PYTHONUNBUFFERED=1

CMD ["/bin/sh", "-c", "nginx && exec python -u run.py"]
