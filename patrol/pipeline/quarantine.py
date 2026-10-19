from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from patrol.options import Options

logger = logging.getLogger(__name__)


def read_delivery_count(
    client: Any,
    topic: str,
    group_id: str,
    event_ref: str,
) -> int:
    """查询指定消息在 pending entries list 里的投递次数。"""
    try:
        entries = client.xpending_range(
            name=topic,
            groupname=group_id,
            min=event_ref,
            max=event_ref,
            count=1,
        )
        if entries:
            return int(entries[0].get("times_delivered", 0))
    except Exception:
        logger.exception("查询投递次数失败 topic=%s event_ref=%s，视为 0", topic, event_ref)
    return 0


def drop_job(
    client: Any,
    group_id: str,
    topic: str,
    entry_id: str,
    job_id: str,
    delivery_count: int,
    options: Options,
    *,
    job_kind: str | None = None,
    digest_ref: str | None = None,
) -> None:
    """
    超过最大重试次数时放弃作业：
    1. MySQL status=9，记录失败原因
    2. XACK 原消息，从 pending 清除，不再重试
    """
    from patrol.brokers.ledger import mark_digest_failed, mark_failed

    error_msg = f"超过最大重试次数，已放弃 delivery_count={delivery_count}"

    if job_kind == "digest.compose" and digest_ref:
        mark_digest_failed(options, digest_ref, error_msg)
    else:
        mark_failed(options, job_id, error_msg)

    try:
        client.xack(topic, group_id, entry_id)
        client.xdel(topic, entry_id)
    except Exception:
        logger.exception("XACK/XDEL 失败 job_id=%s entry_id=%s，消息将继续留在 pending", job_id, entry_id)
        return

    logger.warning(
        "作业已放弃 job_ref=%s digest_ref=%s topic=%s entry_id=%s delivery_count=%d",
        job_id, digest_ref or "n/a", topic, entry_id, delivery_count,
    )
