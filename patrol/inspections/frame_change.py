from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from patrol.messages.envelope import JobEnvelope, Outcome
from patrol.toolkit.pixel_diff import diff_frames
from patrol.toolkit.asset_pull import acquire_asset, drop_temp

if TYPE_CHECKING:
    from patrol.options import Options

logger = logging.getLogger(__name__)


def handle(job: JobEnvelope, options: Options) -> Outcome:
    """两帧对比：CLAHE + Sobel + Otsu 算出变化面积占比。"""
    has_before = bool(job.before_asset_path or job.before_asset_uri)
    has_after = bool(job.after_asset_path or job.after_asset_uri)

    if not has_before or not has_after:
        raise ValueError("vision.frame_change 作业缺少 before/after 两帧地址")

    before_path, before_temp = acquire_asset(job.before_asset_path, job.before_asset_uri)
    after_path, after_temp = acquire_asset(job.after_asset_path, job.after_asset_uri)

    try:
        threshold = int(job.threshold) if job.threshold is not None else 25
        result = diff_frames(
            before_path,
            after_path,
            thresh=threshold,
            auto_thresh=True,
        )
    finally:
        drop_temp(before_path, before_temp)
        drop_temp(after_path, after_temp)

    if not result.ok:
        raise RuntimeError(f"帧间对比失败: {result.error}")

    summary = f"ratio={result.ratio:.2f}% conclusion={result.conclusion} threshold={threshold}"

    logger.info(
        "frame_change 完成 job_ref=%s station=%s %s",
        job.job_ref, job.station_no or "n/a", summary,
    )

    return Outcome(
        accepted=result.conclusion == "change",
        handler="vision.frame_change",
        summary=summary,
    )
