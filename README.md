# vision-patrol-node

队列驱动的视觉巡检工作节点（demo）。从消息队列取作业 → 抢处理权 → 跑视觉算子 → 结果写进账本库 → 把需要大模型深加工的作业交给下游工作流平台。

> **演示性质**：仓库内的表名、队列名、环境变量名、下游地址都是自洽的占位约定，不与任何现网服务对接；`docker/.env.example` 里的 registry、host 全部是假值。要接到真实后端，需要自己改队列 key、建表 DDL 和 webhook 地址。

---

## 1. 为什么这么设计

巡检设备的摄像头每隔几分钟出一帧图，服务端要知道三件事：画面里有没有目标、和上一帧比有没有变化、一天下来发生了什么。这三件事算力差别很大——单帧推理是 CPU 密集，查库拼报告是 IO 密集，扔给大模型总结更是纯等待。

于是把重活从服务端拆出来，做成一个可以横向复制的消费节点：

- **队列**用 Redis Streams + consumer group：多节点天然分工，未 ACK 的消息进 pending，超时能被别人认领，不需要自己写任务表轮询。
- **账本**用 MySQL：每个作业一行，状态推进全记录在上面，前端直接查它，不查 Redis。
- **视觉算子**用 OpenCV（MobileNet SSD + CLAHE/Sobel 差分）：不依赖 GPU，进程池里跑得动。
- **深加工**交给外部工作流平台：本节点只发 HTTP 触发、拿受理回执，不等它跑完；结果由平台回调写回账本。

---

## 2. 快速开始

```bash
git clone <本仓库> && cd vision-patrol-node
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 本地起一份 Redis + MySQL（任意方式都行，这里只是最省事的写法）
docker run -d -p 6379:6379 redis:7-alpine
docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=dev -e MYSQL_DATABASE=vision_patrol mysql:8
mysql -h127.0.0.1 -uroot -pdev vision_patrol < sql/demo_schema.sql   # 表结构 + 几条自测数据

cp .env.example .env      # 按里面的 [必填] 标注补齐
make check                # 语法 + 导入自检，不连外部服务
make run                  # 等价于 python -u run.py
```

任一 `[必填]` 缺失会在启动阶段直接抛错并指名缺哪个 key，不会静默用默认值跑起来。

依赖的外部服务只有三个：Redis（或 Redis Cluster）、MySQL、一个能被 POST 的 webhook 端点。账本表结构见 `sql/demo_schema.sql`，代码里的表名列名与它对齐；脚本里的 INSERT 只为让汇总作业能取出非空快照，真实环境由上游投递方写。

---

## 3. 目录

```
run.py                       进程入口：载 .env → 校验配置 → 起补偿线程 → 进消费循环
Makefile                     本地/构建入口的薄封装
scripts/build-image.sh       镜像构建与推送（build / build-amd64 / push / up / down / logs）
requirements.txt
.env.example                 全部环境变量的说明与必填标注
sql/demo_schema.sql          演示表结构（含 patrol_job_ledger / patrol_digest_record 与几条自测数据）
.github/workflows/image.yml  PR 只构建不推；main/develop 推 ghcr

docker/
├── Dockerfile               python:3.10-slim + nginx + opencv 运行库
├── docker-compose.yml       服务名 patrol-node，容器名 patrol-node-1
├── nginx.conf               9090 探活
├── .env.example             容器侧配置（默认关掉本地视觉，走纯转发）
└── README.md                部署细节

patrol/
├── env.py                   环境变量原语：必填/整数/布尔读取与报错
├── options.py               汇总成 Options，启动时一次性校验
├── logkit.py                loguru 初始化，标准库 logging 拦截转发
├── messages/
│   ├── envelope.py          JobEnvelope（一条消息）· Outcome（一次处理结论）
│   └── errors.py            EnvelopeError：消息体不合法
├── brokers/
│   ├── streams.py           单机/集群连接 · hash tag 对齐的租约 key
│   ├── ledger.py            账本状态推进（尽力而为，写库失败不影响主流程）
│   └── workflow.py          工作流网关：多端点轮询 + 限流 + 重试 + 熔断
├── pipeline/
│   ├── supervisor.py        消费主循环、双执行池调度
│   ├── decode.py            Stream 字段 → JobEnvelope
│   ├── lease_keeper.py      租约后台续期
│   ├── quarantine.py        投递次数判定与死信放弃
│   └── callback_sweeper.py  status=4 超时补偿扫描
├── inspections/
│   ├── router.py            job_kind 路由 + 功能开关 + 工作流出口
│   ├── subject_scan.py      单帧目标识别
│   ├── frame_change.py      两帧变化占比
│   └── period_digest.py     周期汇总：查库拼快照 → 交工作流
├── toolkit/
│   ├── asset_pull.py        本地路径 / URL 取图，临时文件用后即删
│   ├── model_store.py       SSD 权重缺失时自动下载
│   ├── object_scan.py       letterbox · 多尺度 · NMS · 网络缓存
│   ├── pixel_diff.py        CLAHE · Sobel · Otsu · 轮廓面积
│   └── jitter.py            指数退避 + ±10% jitter
```

---

## 4. 分层

| 层 | 负责什么 | 允许依赖 |
|---|---|---|
| `env` / `options` / `logkit` | 读配置、校验、统一日志 | 无内部依赖 |
| `messages` | 纯数据结构与业务异常 | 无内部依赖 |
| `toolkit` | 取图、权重、两套视觉算法、退避 | 无内部依赖 |
| `brokers` | Redis / MySQL / 工作流的封装 | options · messages |
| `inspections` | 每种 `job_kind` 一个处理函数 | messages · brokers · toolkit |
| `pipeline` | 取消息、抢租约、派发、重试、放弃、补偿 | 其余全部 |

依赖方向单向往下，`pipeline` 是唯一有状态的一层。

---

## 5. 一条作业的一生

```
投递方 XADD
  │
  ├─ patrol_job_ledger 插入一行 status=0
  ▼
supervisor 每轮循环
  ├─ XAUTOCLAIM  接管 idle > STALE_AFTER_MS 的 pending 消息
  │     ├─ 已过 expires_at     → status=9 → XACK → XDEL
  │     ├─ 投递次数 > JOB_MAX_DELIVERIES → status=9 → XACK → XDEL（死信）
  │     └─ 否则按重试再处理一遍
  └─ XREADGROUP  阻塞 POLL_BLOCK_MS 等新消息，投给执行池
        ▼
_run_one（进程池子进程 或 线程池线程）
  1. 过期检查                     过期 → status=9 → XACK → XDEL，不占重试额度
  2. SET NX EX 抢租约             抢不到就返回，消息留在 pending
  3. mark_running                 status=1，同时记 runner_id（哪个节点在处理）
  4. LeaseKeeper.watch()          执行期间每 TTL/3 秒续租
  5. route_job                    按 job_kind 进对应算子
        ├─ 本地视觉出结果          → status=2（未命中）/ status=3（命中）
        └─ 开关关闭，转交工作流      → status=4（等平台回调）
  6. XACK → XDEL → DEL 租约
```

**失败路径**故意什么都不做：不 XACK、不 XDEL，消息留在 pending 等下一轮被认领；但租约会立刻删掉，不等 TTL——否则别的节点在 TTL 内根本抢不到，重试就被拖死了。

---

## 6. 队列契约

往 `JOB_TOPIC` 写消息时用这些字段（值都是字符串）：

| 字段 | 必填 | 说明 |
|---|---|---|
| `job_ref` | 是 | 幂等键，与账本里的 `job_ref` 一致；兼容旧写法 `job_id` |
| `job_kind` | 是 | `vision.subject_scan` / `vision.frame_change` / `digest.compose` |
| `station_no` | 否 | 点位编号，兼容旧写法 `station_id` |
| `asset_path` / `asset_uri` | 单帧算子二选一 | 本地路径直接读，URL 会下到临时文件 |
| `before_asset_*` / `after_asset_*` | 差分算子各二选一 | 前后两帧 |
| `confidence` | 否 | 识别置信度阈值，默认 `0.4` |
| `threshold` | 否 | 差分像素阈值，默认 `25` |
| `expires_at` | 否 | Unix 秒；缺省视为永不过期 |
| `capture_kind` / `frame_ref` / `user_uuid` / `user_timezone` / `user_language` | 否 | 透传给下游工作流 |

缺 `job_ref` 或 `job_kind` 会抛 `EnvelopeError`，本轮循环记一条 warning 后跳过，不会让消费线程挂掉。

汇总作业（`digest.compose`）另需 `digest_ref` 和 `user_id`。

---

## 7. 三个算子

### `vision.subject_scan` — 单帧目标识别

```
acquire_asset      本地路径 or URL 下载
ensure_bundle      权重缺失时从 GitHub 拉 MobileNetSSD（proto + caffemodel）
scan_subjects      letterbox 到 300×300 → blobFromImage → 前向
                   → 多尺度（1.0 / 1.2 / 1.5）→ 可选分块 → NMS 去重
                   网络对象在进程内缓存，一个进程只加载一次
```

返回 `detected` / `count` / `labels`，命中写 status=3，未命中写 status=2。演示里关注类别固定在猫和狗（VOC 类），换类别改 `toolkit/object_scan.py` 顶部的类别白名单即可。

### `vision.frame_change` — 两帧差异占比

```
灰度 → CLAHE 光照归一化 → 高斯模糊 → Sobel 边缘增强
     → absdiff → Otsu 自动二值化 → 膨胀 → 找轮廓 → 累计变化面积
```

输出 `ratio`（变化面积占比 %）与 `conclusion=change/no_change`。CLAHE 这一步是为了让夜间补光、光照渐变不被误判成"有变化"。

### `digest.compose` — 周期汇总

不跑视觉，走的是另一条链：

1. 按 `user_id` + 用户时区圈出今天的窗口，从 `patrol_capture_plan` / `patrol_station` / `patrol_capture_log` / `patrol_station_event` 等表拉出计划、抓取统计、事件明细，拼成结构化快照。
2. 先 `_persist_gathered` 把快照落到 `patrol_digest_record`（`job_state=1`，`remark=gathered_waiting_workflow`）——这样即使下一步工作流挂了，采集结果也已经在库里可查。
3. 通过 `WorkflowGateway` POST 给 `DIGEST_WORKFLOW_URLS`，拿回 `title / period_body / rule_body / next_day_note` 等字段写入同一条记录，`job_state=2`。
4. 重入时先查 `job_state`，已经是 `2` 就直接返回成功，不重复烧一次大模型。

快照里的每个 JSON 字段都是独立列，排查"报告内容不对还是数据取错了"时能直接定位到某一块。

---

## 8. 并发模型

两个池子是刻意分开的：

| 池 | 由哪个开关决定 | 配置项 | 为什么 |
|---|---|---|---|
| 主池 | `PIPELINE_RUN_MODE` | `PIPELINE_WORKERS` | 视觉推理吃 CPU，`process` 模式绕开 GIL；纯转发场景切 `thread` 省内存 |
| 汇总池 | 固定 `ThreadPoolExecutor` | `DIGEST_WORKERS`（默认 4） | 查库 + 发 HTTP，纯 IO；和主池隔离后不会因为一条慢汇总堵住视觉作业 |

```env
PIPELINE_RUN_MODE=process     # CPU 核数 - 1
PIPELINE_WORKERS=3
```

```env
PIPELINE_RUN_MODE=thread      # 纯转发节点
PIPELINE_WORKERS=6
```

主池在 `process` 模式下，子进程会重新初始化日志并自建 Redis 连接，与主进程完全隔离——所以 `Options`、`JobEnvelope` 必须是纯数据 dataclass，能 pickle 过进程边界。

多节点扩容优先加容器而不是加单容器并发：每个容器一个唯一 `STREAM_MEMBER`，单容器崩了不影响其他节点，滚动重启期间消费不断。

---

## 9. 租约与时间参数

consumer group 保证一条消息只派给一个成员，但不保证业务不重复执行（同一 `job_ref` 可能被投递两次）。所以处理入口再抢一次 `SET NX EX`：

```
抢租约        SET lease:{shard}:{job_ref} member_id NX EX LEASE_TTL_SEC
续租节奏      每 LEASE_TTL_SEC / 3 秒一次
租约 key      cluster 模式下从 topic 名里取 {hash tag}，保证与队列 key 同 slot
```

必须满足的关系：

```
STALE_AFTER_MS > LEASE_TTL_SEC × 1000
  # 否则作业还在跑（租约还在续），消息就被别的节点接管了

最大执行时长 = LEASE_TTL_SEC / 3 × (LEASE_MAX_RENEWALS + 1)
  # 例：TTL=30s、上限 6 次 → 10 × 7 = 70s 后停止续租
  # 租约最多再撑 30s 自然过期，然后被 XAUTOCLAIM 接管

总尝试次数 = JOB_MAX_DELIVERIES + 1
```

`LEASE_MAX_RENEWALS=0` 表示无限续租，适合线程模式下的长任务；进程模式建议设一个有限值，相当于给作业加了个软超时——失控的作业不会永远占着租约和进程槽位。

---

## 10. 账本状态

`patrol_job_ledger`，按 `job_ref` 定位单行：

| status | 含义 | 谁写的 |
|---|---|---|
| `0` | 已入队 | 上游投递方 |
| `1` | 处理中（抢到租约） | 本节点，同时写 `runner_id` |
| `2` | 视觉判定未通过 | 本节点 |
| `3` | 视觉判定通过 | 本节点 |
| `4` | 已提交工作流，等回调 | 本节点 |
| `9` | 失败终态（过期 / 超过重试 / 回调超时） | 本节点 |

`failure_note` 超 500 字符会截断加 `...`。

**写库是尽力而为**：账本连不上时只记异常日志，不回滚队列侧的动作。消息已经 ACK 就是已经处理完，账本少一行状态靠日志和补偿扫描去对，而不是把消费循环卡死。

`patrol_digest_record` 单独一套 `job_state`：`0` 待处理/重试、`1` 快照已采集等工作流、`2` 完成、`9` 失败。

---

## 11. 下游交接与补偿

**单帧作业转交**（`router.py`）：`VISION_ENABLED` 或对应子开关关闭时，整条作业原样 POST 到 `WORKFLOW_WEBHOOK_URL`。上游字段是**透传**的——不维护白名单，新增业务字段不用回来改这里；标准字段与历史别名会在覆盖时统一成规范键名。

只等 HTTP 受理回执（`WORKFLOW_ACK_TIMEOUT_SEC`，默认 10s），不等业务处理完成。状态码 >= 400 会抛异常，走失败重试路径。

**汇总作业转交**（`brokers/workflow.py`）：支持多个端点，`DIGEST_WORKFLOW_BALANCE=round_robin` 时按顺序轮转，其余取值固定取第一个未熔断的端点；`DIGEST_WORKFLOW_MAX_INFLIGHT` 是信号量上限，超了就排队而不是打爆下游；`DIGEST_WORKFLOW_RETRIES` 只重试可重试的状态码（5xx、408、429）；连续失败到 `DIGEST_WORKFLOW_FAILURE_THRESHOLD` 次会把该端点熔断 `DIGEST_WORKFLOW_COOLDOWN_SEC` 秒，全部熔断时本轮直接抛错走重试路径。

**回调补偿**（`callback_sweeper.py`）：作业转交后账本停在 `4`，正常应由回调方改写成终态。回调丢了就会永远卡在 `4`，所以起一个后台线程定期扫：

```sql
SELECT job_ref, update_time FROM patrol_job_ledger
WHERE status = '4' AND update_time < NOW() - INTERVAL ? MINUTE
```

批量改成 `9` 并写原因。阈值经验值 = 下游平均处理时长（分钟）× 3。`SWEEP_ENABLED=false` 时线程根本不启动，卡住的作业就靠人工处理。

---

## 12. 功能开关

| 变量 | 默认 | 关掉之后的行为 |
|---|---|---|
| `VISION_ENABLED` | `true` | 所有 `vision.*` 作业跳过本地推理，直接转工作流 |
| `VISION_SUBJECT_ENABLED` | `true` | 只有 `vision.subject_scan` 跳过 |
| `VISION_DIFF_ENABLED` | `true` | 只有 `vision.frame_change` 跳过 |
| `SWEEP_ENABLED` | `true` | 不起补偿线程，`status=4` 不会被自动兜底 |

总开关和子开关任一关闭都会走转交，效果相同；两个子开关彼此独立。`digest.compose` 不受视觉开关影响——它本来就不做推理。

容器侧 `docker/.env.example` 默认把三个视觉开关置 `false`，那份镜像的定位是纯转发节点。

---

## 13. 加一类作业要动几个文件

1. `patrol/inspections/` 新建一个处理函数文件，签名 `handle(job: JobEnvelope, options: Options) -> Outcome`。
2. 在 `router.py` 的 `_ROUTES` 里登记 `(模块.handle, 开关属性名或 None)`。
3. 需要新字段就往 `messages/envelope.py` 的 `JobEnvelope` 加，并在 `pipeline/decode.py` 里从原始字典取值。
4. 需要新配置就加 `options.py` 字段 + `env.py` 读取 + `.env.example` 说明。

调度层、租约、重试、账本都不用碰。

---

## 14. 探活与日志

进程本身不起 HTTP 服务：9090 完全由 nginx sidecar 持有，`/health` 固定 200、`/status` 是 stub_status，只能证明容器活着。`CMD` 里的 `exec` 让 Python 作为 1 号进程的直接子进程接住 SIGTERM，消费循环能优雅收尾。至于队列消费是否正常，看账本状态有没有推进、日志里有没有 `作业状态已更新` 更直接。

日志统一走 loguru：stdout 上色，设了 `LOG_DIR` 时主进程额外落盘（子进程不写同一个文件，避免并发抢写），按 50 MB 切割、保留 14 天。redis-py、pymysql 这些第三方库的标准 logging 由 `InterceptHandler` 转投进同一套格式。

排障时先看这三个字段：`job_ref`（跨系统唯一）、`entry_id`（Redis 消息 id）、`runner_tag`（哪个进程/线程处理的，形如 `process/pid=12345` 或 `digest-thread/period-digest_0`）。

---

## 15. 部署

```bash
# 构建（镜像标签自动取当前分支名，斜杠转连字符；非 git 目录退回 latest）
make build-amd64
make push            # REGISTRY / IMAGE_NAME / IMAGE_TAG 三个变量决定推去哪

# 起容器
cp docker/.env.example docker/.env && vi docker/.env
make up
make logs
```

细节（多实例要改哪几处、端口冲突、数据卷路径）见 `docker/README.md`。

GitHub Actions 里 PR 只验证能构建，`main` / `develop/**` 推到 `ghcr.io`。

---

## 16. 已知边界

- **只做 `UPDATE`，不做 `INSERT`**：账本行由上游投递方建，本节点不会补写，因此上游漏插一行时这里只会出现"更新未命中"的 warning 日志。
- **结果回写未完成**：工作流回调后写回哪些业务字段（消息号、通知号等）还没定，目前只由补偿扫描兜底成 `9`。
- **没有测试**：`make check` 只覆盖语法与导入，算子精度、租约竞态、退避行为都还没有自动化验证。
- **演示配置不与真实服务对接**：见文首说明。
