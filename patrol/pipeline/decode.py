from __future__ import annotations

from patrol.messages.errors import EnvelopeError
from patrol.messages.envelope import JobEnvelope, to_optional_float


def _parse_expires_at(fields: dict[str, str]) -> float | None:
    raw = fields.get("expires_at")
    if not raw:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def decode_envelope(topic: str, entry_id: str, fields: dict[str, str]) -> JobEnvelope:
    """将 Redis Stream 原始字段解析并校验为内部作业对象。"""
    # job_ref 为主键；兼容上游旧字段名 job_id
    job_ref = fields.get("job_ref") or fields.get("job_id")
    job_kind = fields.get("job_kind")

    if not job_ref:
        raise EnvelopeError("作业缺少 job_ref")
    if not job_kind:
        raise EnvelopeError("作业缺少 job_kind")

    return JobEnvelope(
        topic=topic,
        entry_id=entry_id,
        job_ref=job_ref,
        job_kind=job_kind,
        station_no=fields.get("station_no") or fields.get("station_id"),
        user_uuid=fields.get("user_uuid"),
        frame_ref=fields.get("frame_ref"),
        user_timezone=fields.get("user_timezone"),
        user_language=fields.get("user_language"),
        capture_kind=fields.get("capture_kind"),
        asset_uri=fields.get("asset_uri"),
        asset_path=fields.get("asset_path"),
        before_asset_uri=fields.get("before_asset_uri"),
        before_asset_path=fields.get("before_asset_path"),
        after_asset_uri=fields.get("after_asset_uri"),
        after_asset_path=fields.get("after_asset_path"),
        confidence=to_optional_float(fields.get("confidence")),
        threshold=to_optional_float(fields.get("threshold")),
        expires_at=_parse_expires_at(fields),
        raw=fields,
    )
