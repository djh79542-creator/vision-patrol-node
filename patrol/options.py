from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from patrol.env import (
    resolve_member_name,
    optional_bool,
    optional_int,
    require_env,
    require_int,
    read_env,
)

StreamMode = Literal["standalone", "cluster"]
RunMode = Literal["process", "thread"]
DEFAULT_DIGEST_TOPIC = "patrol:digest:queue"


def _digest_topic() -> str:
    return read_env("DIGEST_TOPIC") or DEFAULT_DIGEST_TOPIC


def _parse_run_mode() -> RunMode:
    raw = (read_env("PIPELINE_RUN_MODE") or "process").lower()
    if raw not in {"process", "thread"}:
        raise ValueError(f"不支持的 PIPELINE_RUN_MODE: {raw}，只允许 process / thread")
    return raw  # type: ignore[return-value]


def _resolve_topics() -> tuple[str, ...]:
    # JOB_TOPIC / JOB_TOPIC_LIST 都接受，汇总队列始终占一个位
    digest_name = _digest_topic()
    raw_topics = read_env("JOB_TOPIC_LIST")
    if raw_topics:
        topics = [item.strip() for item in raw_topics.split(",") if item.strip()]
    else:
        topics = [require_env("JOB_TOPIC")]

    if digest_name and digest_name not in topics:
        topics.append(digest_name)
    return tuple(topics)


def _digest_workflow_urls() -> tuple[str, ...]:
    raw_urls = read_env("DIGEST_WORKFLOW_URLS")
    if raw_urls:
        urls = tuple(item.strip() for item in raw_urls.split(",") if item.strip())
        if urls:
            return urls

    single_url = read_env("DIGEST_WORKFLOW_URL")
    return (single_url,) if single_url else ()


def _parse_cluster_nodes() -> tuple[tuple[str, int], ...]:
    raw_nodes = read_env("STREAM_CLUSTER_NODES")
    if not raw_nodes:
        return ()

    nodes: list[tuple[str, int]] = []
    for raw_node in raw_nodes.split(","):
        item = raw_node.strip()
        if not item:
            continue

        if ":" not in item:
            raise ValueError(f"STREAM_CLUSTER_NODES 节点格式错误: {item}")

        host, port_text = item.rsplit(":", 1)
        try:
            port = int(port_text)
        except ValueError as error:
            raise ValueError(f"STREAM_CLUSTER_NODES 端口不是合法整数: {item}") from error

        nodes.append((host.strip(), port))

    return tuple(nodes)


def _load_single_node_fields() -> tuple[str, int, int]:
    return (
        require_env("STREAM_HOST"),
        require_int("STREAM_PORT"),
        require_int("STREAM_DB"),
    )


@dataclass(slots=True)
class Options:
    stream_mode: StreamMode
    # standalone 专用；cluster 模式下为 None
    stream_host: str | None
    stream_port: int | None
    stream_db: int | None
    stream_user: str | None
    stream_token: str | None
    # cluster 专用；standalone 模式下为空 tuple
    stream_cluster_nodes: tuple[tuple[str, int], ...]
    job_topic: str
    digest_topic: str
    job_topics: tuple[str, ...]
    stream_group: str
    member_name: str
    lease_prefix: str
    lease_ttl_sec: int
    poll_block_ms: int
    stale_after_ms: int
    max_deliveries: int
    # MySQL：密码可为空，其余必填
    ledger_host: str
    ledger_port: int
    ledger_user: str
    ledger_token: str | None
    ledger_schema: str
    pipeline_workers: int
    digest_workers: int
    poll_count: int
    bundle_dir: str
    # 运行模式：
    # process - ProcessPoolExecutor，绕过 GIL，适合 CPU 密集型 Vision 推理
    # thread  - ThreadPoolExecutor，共享内存，适合 IO 密集型纯 Workflow HTTP 转发
    run_mode: RunMode
    # 租约续期上限：0=不限；进程模式下建议设为 3
    lease_max_renewals: int
    # 功能开关：总开关关闭时，所有 vision.* 作业直接跳过
    vision_enabled: bool
    subject_scan_enabled: bool
    frame_change_enabled: bool
    # Workflow 接入配置
    workflow_webhook_url: str
    digest_workflow_url: str | None
    digest_workflow_urls: tuple[str, ...]
    workflow_ack_timeout_sec: int
    digest_workflow_timeout_sec: int
    digest_workflow_max_inflight: int
    digest_workflow_retries: int
    digest_workflow_balance: str
    digest_workflow_failure_threshold: int
    digest_workflow_cooldown_sec: int
    # Workflow 回调补偿开关及参数
    # 关闭时不启动 keeper 线程，status=4 的作业不会被自动补偿
    sweep_enabled: bool
    sweep_stale_minutes: int
    sweep_interval_sec: int


def load_options() -> Options:
    stream_mode = require_env("STREAM_MODE").strip().lower()
    if stream_mode not in {"standalone", "cluster"}:
        raise ValueError(f"不支持的 STREAM_MODE: {stream_mode}，只允许 standalone / cluster")

    job_topics = _resolve_topics()
    stream_cluster_nodes = _parse_cluster_nodes()

    if stream_mode == "cluster" and not stream_cluster_nodes:
        raise ValueError("STREAM_MODE=cluster 时必须提供 STREAM_CLUSTER_NODES")

    if stream_mode == "standalone":
        host, port, database = _load_single_node_fields()
    else:
        host, port, database = None, None, None

    return Options(
        stream_mode=stream_mode,
        stream_host=host,
        stream_port=port,
        stream_db=database,
        stream_user=read_env("STREAM_USER"),
        stream_token=read_env("STREAM_TOKEN"),
        stream_cluster_nodes=stream_cluster_nodes,
        job_topic=job_topics[0],
        digest_topic=_digest_topic(),
        job_topics=job_topics,
        stream_group=require_env("STREAM_GROUP"),
        member_name=resolve_member_name(),
        lease_prefix=require_env("LEASE_PREFIX"),
        lease_ttl_sec=require_int("LEASE_TTL_SEC"),
        poll_block_ms=require_int("POLL_BLOCK_MS"),
        stale_after_ms=require_int("STALE_AFTER_MS"),
        max_deliveries=optional_int("JOB_MAX_DELIVERIES", default=3),
        ledger_host=require_env("LEDGER_HOST"),
        ledger_port=optional_int("LEDGER_PORT", default=3306),
        ledger_user=require_env("LEDGER_USER"),
        ledger_token=read_env("LEDGER_TOKEN"),
        ledger_schema=require_env("LEDGER_SCHEMA"),
        pipeline_workers=optional_int("PIPELINE_WORKERS", default=1),
        digest_workers=optional_int("DIGEST_WORKERS", default=4),
        poll_count=optional_int("POLL_COUNT", default=1),
        run_mode=_parse_run_mode(),
        bundle_dir=read_env("BUNDLE_DIR") or "./models",
        lease_max_renewals=optional_int("LEASE_MAX_RENEWALS", default=0),
        vision_enabled=optional_bool("VISION_ENABLED", default=True),
        subject_scan_enabled=optional_bool("VISION_SUBJECT_ENABLED", default=True),
        frame_change_enabled=optional_bool("VISION_DIFF_ENABLED", default=True),
        workflow_webhook_url=require_env("WORKFLOW_WEBHOOK_URL"),
        digest_workflow_url=read_env("DIGEST_WORKFLOW_URL"),
        digest_workflow_urls=_digest_workflow_urls(),
        workflow_ack_timeout_sec=optional_int("WORKFLOW_ACK_TIMEOUT_SEC", default=10),
        digest_workflow_timeout_sec=optional_int("DIGEST_WORKFLOW_TIMEOUT_SEC", default=60),
        digest_workflow_max_inflight=optional_int("DIGEST_WORKFLOW_MAX_INFLIGHT", default=4),
        digest_workflow_retries=optional_int("DIGEST_WORKFLOW_RETRIES", default=2),
        digest_workflow_balance=read_env("DIGEST_WORKFLOW_BALANCE") or "round_robin",
        digest_workflow_failure_threshold=optional_int("DIGEST_WORKFLOW_FAILURE_THRESHOLD", default=5),
        digest_workflow_cooldown_sec=optional_int(
            "DIGEST_WORKFLOW_COOLDOWN_SEC",
            default=60,
        ),
        sweep_enabled=optional_bool("SWEEP_ENABLED", default=True),
        sweep_stale_minutes=optional_int("SWEEP_STALE_MINUTES", default=30),
        sweep_interval_sec=optional_int("SWEEP_INTERVAL_SEC", default=120),
    )
