.PHONY: run run-thread check build build-amd64 push up down logs clean

# 裸机运行：先 cp .env.example .env 填好必填项
run:
	python -u run.py

# 纯转发场景（不做本地视觉推理）用线程池更省资源
run-thread:
	PIPELINE_RUN_MODE=thread python -u run.py

# 语法自检 + 依赖导入冒烟，不连外部服务
check:
	python -m compileall -q patrol run.py
	python -c "import run, patrol.options, patrol.pipeline.supervisor; print('import ok')"

build:
	bash scripts/build-image.sh build

build-amd64:
	bash scripts/build-image.sh build-amd64

push:
	bash scripts/build-image.sh push

up:
	bash scripts/build-image.sh up

down:
	bash scripts/build-image.sh down

logs:
	bash scripts/build-image.sh logs

clean:
	bash scripts/build-image.sh clean
