from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pymysql
import pymysql.cursors

if TYPE_CHECKING:
    from patrol.options import Options

logger = logging.getLogger(__name__)

JOB_TABLE = "patrol_job_ledger"
DIGEST_TABLE = "patrol_digest_record"

# status 取值（终态只有 2/3/9，其余是过程态）：
#   0 已入队，等待节点认领
#   1 处理中，某个节点刚抢到租约
#   2 Vision 判定未通过：跑完推理，没看到关注对象
#   3 Vision 判定通过：跑完推理，看到关注对象
#   4 已提交 Workflow，等回调方写回终态
#   9 失败终态：重试耗尽 / 作业过期 / 回调超时


def open_ledger(options: Options) -> pymysql.Connection:
    return pymysql.connect(
        host=options.ledger_host,
        port=options.ledger_port,
        user=options.ledger_user,
        password=options.ledger_token,
        database=options.ledger_schema,
        charset="utf8mb4",
        connect_timeout=5,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _write_job_state(options: Options, job_ref: str, status: str, failure_note: str | None) -> None:
    """通用状态更新，MySQL 操作尽力而为：不可达时只打日志，不影响主流程。"""
    truncated: str | None = None
    if failure_note:
        truncated = failure_note[:497] + "..." if len(failure_note) > 500 else failure_note

    try:
        conn = open_ledger(options)
        with conn:
            with conn.cursor() as cursor:
                affected = cursor.execute(
                    f"""
                    UPDATE {JOB_TABLE}
                    SET    status       = %s,
                           failure_note = %s,
                           update_time  = NOW()
                    WHERE  job_ref      = %s
                    """,
                    (status, truncated, job_ref),
                )
                if affected == 0:
                    logger.warning("更新作业状态未命中 status=%s job_ref=%s", status, job_ref)
                else:
                    logger.info("作业状态已更新 status=%s job_ref=%s", status, job_ref)
    except Exception:
        logger.exception("MySQL 更新作业状态失败，作业状态未同步 job_ref=%s", job_ref)


def mark_running(options: Options, job_ref: str, runner_id: str) -> None:
    """抢到租约、开始处理，标记为处理中（status=1）。"""
    try:
        conn = open_ledger(options)
        with conn:
            with conn.cursor() as cursor:
                affected = cursor.execute(
                    f"""
                    UPDATE {JOB_TABLE}
                    SET    status      = '1',
                           runner_id   = %s,
                           update_time = NOW()
                    WHERE  job_ref     = %s
                    """,
                    (runner_id, job_ref),
                )
                if affected == 0:
                    logger.warning("更新作业状态未命中 status=1 job_ref=%s", job_ref)
                else:
                    logger.info("作业状态已更新 status=1 job_ref=%s runner=%s", job_ref, runner_id)
    except Exception:
        logger.exception("MySQL 更新作业状态失败，作业状态未同步 job_ref=%s", job_ref)


def mark_finished(options: Options, job_ref: str, accepted: bool) -> None:
    """
    Vision 步骤完成：
    - accepted=True  → status=3（命中关注对象）
    - accepted=False → status=2（未命中关注对象）
    """
    status = "3" if accepted else "2"
    _write_job_state(options, job_ref, status, None)


def mark_handoff_pending(options: Options, job_ref: str) -> None:
    """作业已提交 Workflow，标记为 Workflow 处理中（status=4）。"""
    _write_job_state(options, job_ref, "4", None)


def mark_failed(options: Options, job_ref: str, failure_note: str) -> None:
    """超过最大重试次数或其他异常，标记为处理失败（status=9）。"""
    _write_job_state(options, job_ref, "9", failure_note)


def mark_dropped(options: Options, job_ref: str) -> None:
    """作业重试时发现已超过有效期，标记为处理失败（status=9）。"""
    _write_job_state(options, job_ref, "9", "作业已超过有效期，跳过处理")


def _write_digest_state(
    options: Options,
    digest_ref: str,
    status: str,
    remark: str | None = None,
) -> None:
    """Update patrol_digest_record.job_state; failure detail is stored in remark."""
    truncated: str | None = None
    if remark:
        truncated = remark[:497] + "..." if len(remark) > 500 else remark

    try:
        conn = open_ledger(options)
        with conn:
            with conn.cursor() as cursor:
                affected = cursor.execute(
                    f"""
                    UPDATE {DIGEST_TABLE}
                    SET    job_state = %s,
                           remark      = COALESCE(%s, remark),
                           update_time = NOW()
                    WHERE  digest_ref = %s
                    """,
                    (status, truncated, digest_ref),
                )
                if affected == 0:
                    logger.warning("Digest report status update missed status=%s digest_ref=%s", status, digest_ref)
                else:
                    logger.info("Digest report status updated status=%s digest_ref=%s", status, digest_ref)
    except Exception:
        logger.exception("MySQL digest report status update failed digest_ref=%s status=%s", digest_ref, status)


def mark_digest_running(options: Options, digest_ref: str) -> None:
    _write_digest_state(options, digest_ref, "1", None)


def mark_digest_queued(options: Options, digest_ref: str, reason: str | None = None) -> None:
    _write_digest_state(options, digest_ref, "0", reason)


def mark_digest_failed(options: Options, digest_ref: str, failure_note: str) -> None:
    _write_digest_state(options, digest_ref, "9", failure_note)


def mark_digest_dropped(options: Options, digest_ref: str) -> None:
    mark_digest_failed(options, digest_ref, "Job expired; skipped")

