from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import httpx

from patrol.inspections import frame_change, period_digest, subject_scan
from patrol.messages.envelope import JobEnvelope, Outcome

if TYPE_CHECKING:
    from patrol.options import Options

logger = logging.getLogger(__name__)

VISION_PREFIX = "vision."

# 每种作业类型登记一次：处理函数 + 它对应的开关属性名（None 表示不受开关影响）。
# 增加新作业时只动这张表，pipeline 层不需要改。
_ROUTES: dict[str, tuple] = {
    "vision.subject_scan": (subject_scan.handle, "subject_scan_enabled"),
    "vision.frame_change": (frame_change.handle, "frame_change_enabled"),
    "digest.compose": (period_digest.handle, None),
}


def _build_workflow_payload(job: JobEnvelope) -> dict[str, str]:
    # 上游字段全量透传，避免每次新增字段都要回来补白名单；
    # 标准字段与历史别名在这里统一覆盖，保证下游收到的键名稳定。
    payload = dict(job.raw)
    payload["job_ref"] = job.job_ref
    payload["job_kind"] = job.job_kind
    payload["asset_uri"] = job.asset_uri or payload.get("asset_uri") or ""
    payload["capture_kind"] = job.capture_kind or payload.get("capture_kind") or ""
    payload["frame_ref"] = job.frame_ref or payload.get("frame_ref") or ""
    payload["user_language"] = job.user_language or payload.get("user_language") or ""
    payload["station_no"] = job.station_no or payload.get("station_no") or payload.get("station_id") or ""
    payload["user_uuid"] = job.user_uuid or payload.get("user_uuid") or ""
    payload["user_timezone"] = job.user_timezone or payload.get("user_timezone") or ""
    return payload


def _hand_off_workflow(job: JobEnvelope, options: Options) -> Outcome:
    """
    把作业交给外部工作流平台（webhook 触发，不等它跑完）。

    三种情况会走到这里：
    - 视觉总开关 VISION_ENABLED 关闭
    - 单帧识别开关 VISION_SUBJECT_ENABLED 关闭
    - 帧间对比开关 VISION_DIFF_ENABLED 关闭

    平台收到请求后立刻回 200，真正的处理在它那边异步进行，结果由回调写进另一个服务。
    本服务只确认 HTTP 收妥（WORKFLOW_ACK_TIMEOUT_SEC），不等模型出结果。

    2xx → handler="workflow"，账本记 status=4（等平台回调）
    失败 → 抛异常，不做 XACK，消息留在 pending 里等下一轮接管
    """
    payload = _build_workflow_payload(job)

    logger.debug(
        "workflow handoff job_ref=%s payload=%s",
        job.job_ref, payload,
    )

    try:
        response = httpx.post(
            options.workflow_webhook_url,
            json=payload,
            timeout=options.workflow_ack_timeout_sec,
        )
    except httpx.TimeoutException as exc:
        raise RuntimeError(
            f"工作流请求超时 job_ref={job.job_ref} url={options.workflow_webhook_url}"
        ) from exc
    except httpx.RequestError as exc:
        raise RuntimeError(
            f"工作流网络错误 job_ref={job.job_ref} error={exc}"
        ) from exc

    if response.status_code >= 400:
        raise RuntimeError(
            f"工作流返回错误 job_ref={job.job_ref} "
            f"status={response.status_code} body={response.text[:200]}"
        )

    return Outcome(
        handler="workflow",
        accepted=True,
        summary=f"已交给工作流平台，HTTP {response.status_code}",
    )


def _vision_bypassed(job_kind: str, options: Options) -> bool:
    """
    这一类作业要不要跳过本地视觉步骤：
    - 总开关关掉 → 所有 vision.* 都不做，直接交给平台
    - 该功能自己的开关关掉 → 只有它跳过
    两个子开关互不影响，关一个另一个照常跑。
    """
    if not options.vision_enabled:
        return True
    route = _ROUTES.get(job_kind)
    gate_attr = route[1] if route else None
    if gate_attr and not getattr(options, gate_attr, True):
        return True
    return False


def route_job(job: JobEnvelope, options: Options) -> Outcome:
    """
    按 job_kind 分发：

    1. 总开关或对应子开关关闭
       → 跳过本地视觉，整条作业直接交给工作流平台

    2. 开关齐备（常规路径）
       → 由该类型的处理函数完成本地视觉环节
    """
    if job.job_kind.startswith(VISION_PREFIX) and _vision_bypassed(job.job_kind, options):
        return _hand_off_workflow(job, options)

    route = _ROUTES.get(job.job_kind)
    if route is None:
        raise ValueError(f"暂不支持的 job_kind: {job.job_kind}")

    handle_fn = route[0]
    return handle_fn(job, options)
