from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from patrol.messages.envelope import JobEnvelope, Outcome
from patrol.toolkit.object_scan import scan_subjects
from patrol.toolkit.asset_pull import acquire_asset, drop_temp
from patrol.toolkit.model_store import ensure_bundle

if TYPE_CHECKING:
    from patrol.options import Options

logger = logging.getLogger(__name__)


def handle(job: JobEnvelope, options: Options) -> Outcome:
    """单帧主体识别：MobileNet SSD 在画面里找关注对象。"""
    if not job.asset_path and not job.asset_uri:
        raise ValueError("vision.subject_scan 作业缺少 asset_path 或 asset_uri")

    proto_path, model_path = ensure_bundle(options.bundle_dir)

    local_path, is_temp = acquire_asset(job.asset_path, job.asset_uri)
    try:
        confidence = job.confidence if job.confidence is not None else 0.4
        results = scan_subjects(
            local_path,
            proto_path=proto_path,
            model_path=model_path,
            threshold=confidence,
        )
    finally:
        drop_temp(local_path, is_temp)

    detected = len(results) > 0
    labels = list({r.label for r in results})
    summary = f"detected={detected} count={len(results)} labels={','.join(labels) or 'none'}"

    logger.info(
        "subject_scan 完成 job_ref=%s station=%s %s",
        job.job_ref, job.station_no or "n/a", summary,
    )

    return Outcome(
        accepted=detected,
        handler="vision.subject_scan",
        summary=summary,
    )
