from __future__ import annotations

import logging
import os
import signal
import sys
from pathlib import Path

# 强制行缓冲，避免子进程日志在 Windows spawn 模式下延迟或丢失
os.environ.setdefault("PYTHONUNBUFFERED", "1")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]


def _load_dotenv() -> None:
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip())


_load_dotenv()

from patrol.logkit import setup_logging  # noqa: E402
from patrol.options import load_options  # noqa: E402
from patrol.pipeline.supervisor import JobSupervisor  # noqa: E402
from patrol.pipeline.callback_sweeper import CallbackSweeper  # noqa: E402

logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    options = load_options()

    from loguru import logger as _logger  # noqa: PLC0415
    _logger.info(
        "\n"
        "========== vision-patrol node ==========\n"
        " [STREAM]\n"
        "   mode={stream_mode}  nodes={stream_nodes}\n"
        "   topics={topics}\n"
        "   group={group}  member={member}\n"
        " [RUNTIME]\n"
        "   executor={executor}  workers={concurrency}  poll_count={poll_count}\n"
        "   lease_ttl={lease_ttl}s  stale_after={stale_after}ms\n"
        "   max_deliveries={max_deliveries}  max_renewals={max_renewals}\n"
        " [VISION]\n"
        "   master={vision}  subject_scan={subject_scan}  frame_change={frame_change}\n"
        " [WORKFLOW]\n"
        "   webhook={webhook_url}  ack_timeout={webhook_ack_timeout}s\n"
        "   digest_urls={digest_urls}  digest_timeout={digest_timeout}s\n"
        "   digest_max_inflight={digest_max_inflight}\n"
        "   sweeper={sweeper}  stale={sweep_stale}min  interval={sweep_interval}s\n"
        " [LEDGER]\n"
        "   {ledger_host}:{ledger_port}  schema={ledger_schema}\n"
        "=========================================",
        stream_mode=options.stream_mode,
        stream_nodes=options.stream_cluster_nodes or f"{options.stream_host}:{options.stream_port} db={options.stream_db}",
        topics=",".join(options.job_topics),
        group=options.stream_group,
        member=options.member_name,
        executor=options.run_mode,
        concurrency=options.pipeline_workers,
        poll_count=options.poll_count,
        lease_ttl=options.lease_ttl_sec,
        stale_after=options.stale_after_ms,
        max_deliveries=options.max_deliveries,
        max_renewals=options.lease_max_renewals or "unlimited",
        vision="on" if options.vision_enabled else "off",
        subject_scan="on" if options.subject_scan_enabled else "off",
        frame_change="on" if options.frame_change_enabled else "off",
        webhook_url=options.workflow_webhook_url,
        webhook_ack_timeout=options.workflow_ack_timeout_sec,
        digest_urls=",".join(options.digest_workflow_urls) or "unset",
        digest_timeout=options.digest_workflow_timeout_sec,
        digest_max_inflight=options.digest_workflow_max_inflight,
        sweeper="on" if options.sweep_enabled else "off",
        sweep_stale=options.sweep_stale_minutes,
        sweep_interval=options.sweep_interval_sec,
        ledger_host=options.ledger_host,
        ledger_port=options.ledger_port,
        ledger_schema=options.ledger_schema,
    )

    callback_sweeper: CallbackSweeper | None = None
    if options.sweep_enabled:
        callback_sweeper = CallbackSweeper(options)
        callback_sweeper.start()
    else:
        logger.info("CallbackSweeper 已通过配置关闭（SWEEP_ENABLED=false）")

    member = JobSupervisor(options)

    def _on_exit_signal(signum: int, _frame: object | None) -> None:
        # 与 Docker / systemd 的 SIGTERM、终端 Ctrl+C 对齐，先关 Redis 阻塞以尽快退出消费循环
        logger.info("收到退出信号 signum=%s，准备停止 JobSupervisor", signum)
        member.stop()

    signal.signal(signal.SIGINT, _on_exit_signal)
    if hasattr(signal, "SIGTERM"):
        try:
            signal.signal(signal.SIGTERM, _on_exit_signal)
        except (ValueError, OSError):
            logger.warning("当前环境无法注册 SIGTERM，将仅依赖 SIGINT / KeyboardInterrupt")

    try:
        member.start()
    except KeyboardInterrupt:
        logger.info("收到键盘中断，准备退出")
    finally:
        member.stop()
        if callback_sweeper:
            callback_sweeper.stop()


if __name__ == "__main__":
    # 与 Windows 一致使用 spawn，避免 Linux/macOS 默认 fork 与 Windows 行为分叉（pickle/全局状态）
    import multiprocessing as _mp

    try:
        _mp.set_start_method("spawn", force=True)
    except RuntimeError:
        # 极少数嵌入环境已固定 context
        pass
    main()
