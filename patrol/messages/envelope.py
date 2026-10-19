from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class JobEnvelope:
    """从 Redis Stream 解析出的作业数据，供各层传递使用。"""
    topic: str
    entry_id: str
    job_ref: str          # Redis Stream 幂等键，对应 MySQL job_ref
    job_kind: str
    station_no: str | None
    user_uuid: str | None
    frame_ref: str | None
    user_timezone: str | None
    user_language: str | None
    capture_kind: str | None
    asset_uri: str | None
    asset_path: str | None
    before_asset_uri: str | None
    before_asset_path: str | None
    after_asset_uri: str | None
    after_asset_path: str | None
    confidence: float | None
    threshold: float | None
    # Unix 时间戳（秒）；为 None 表示作业不设过期
    expires_at: float | None
    raw: dict[str, str]

    @property
    def job_id(self) -> str:
        """兼容别名，等同于 job_ref。"""
        return self.job_ref

    @property
    def station_id(self) -> str | None:
        """兼容别名，等同于 station_no。"""
        return self.station_no

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at


@dataclass(slots=True)
class Outcome:
    """业务处理层返回给 runner 的处理结果。"""
    accepted: bool
    handler: str
    summary: str


def to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
