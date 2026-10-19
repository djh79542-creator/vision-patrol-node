from __future__ import annotations

import json
import logging
from datetime import datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from patrol.messages.envelope import JobEnvelope, Outcome
from patrol.brokers.workflow import run_digest_workflow
from patrol.brokers.ledger import open_ledger, mark_digest_running

if TYPE_CHECKING:
    from patrol.options import Options

logger = logging.getLogger(__name__)

JOB_KIND = "digest.compose"
PIPELINE_LABEL = "period-digest"
ENTRY_MODEL = "CAM-M1"


def _job_field(job: JobEnvelope, key: str, default: str = "") -> str:
    value = job.raw.get(key)
    return str(value) if value is not None else default


def _day_window(timezone: str) -> tuple[str, str]:
    try:
        tz = ZoneInfo(timezone or "UTC")
    except Exception:
        logger.warning("Invalid user timezone=%s, fallback to UTC", timezone)
        tz = ZoneInfo("UTC")

    now = datetime.now(tz)
    start = datetime.combine(now.date(), time.min, tzinfo=tz)
    end = start + timedelta(days=1)
    return (
        start.strftime("%Y-%m-%d %H:%M:%S"),
        end.strftime("%Y-%m-%d %H:%M:%S"),
    )


def _next_day_weekday(timezone: str) -> int:
    try:
        tz = ZoneInfo(timezone or "UTC")
    except Exception:
        tz = ZoneInfo("UTC")
    tomorrow = datetime.now(tz).date() + timedelta(days=1)
    return tomorrow.isoweekday()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=_json_default)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def _normalize_row(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    return {key: _normalize_value(value) for key, value in row.items()}


def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_normalize_row(row) for row in rows]


def _capture_kind_code(value: Any) -> int:
    raw = str(value or "").strip()
    if raw in {"1", "[1]", "object"}:
        return 1
    if raw in {"2", "[2]", "action"}:
        return 2
    if raw in {"3", "[3]", "stock"}:
        return 3
    if raw in {"4", "[4]", "idle"}:
        return 4
    return 0


def _scheduled_next_day(plan: dict[str, Any], next_day_weekday: int) -> bool:
    cadence = str(plan.get("cadence") or "")
    if cadence == "2":
        return True
    if cadence == "1":
        return False

    weekday_mask = str(plan.get("weekday_mask") or "")
    if weekday_mask:
        enabled_days = {item.strip() for item in weekday_mask.split(",") if item.strip()}
        return str(next_day_weekday) in enabled_days

    return cadence == "0"


def _next_window_preview(open_plans: list[dict[str, Any]], timezone: str) -> dict[str, Any]:
    next_day_weekday = _next_day_weekday(timezone)
    schedules = [
        _normalize_row(plan)
        for plan in open_plans
        if _scheduled_next_day(plan, next_day_weekday)
    ]
    schedules.sort(
        key=lambda item: (
            int(item.get("run_hour") or 0),
            int(item.get("run_minute") or 0),
            str(item.get("station_no") or ""),
        )
    )
    if not schedules:
        return {
            "next_run_at": "",
            "next_run_units": 0,
            "next_run_stations": "",
        }

    first = schedules[0]
    first_hour = int(first.get("run_hour") or 0)
    first_minute = int(first.get("run_minute") or 0)
    tied_slots = [
        item
        for item in schedules
        if int(item.get("run_hour") or 0) == first_hour
        and int(item.get("run_minute") or 0) == first_minute
    ]
    station_labels = []
    for item in tied_slots:
        station_name = str(item.get("station_name") or "")
        station_no = str(item.get("station_no") or "")
        if station_name and station_no:
            station_labels.append(f"{station_name}({station_no})")
        else:
            station_labels.append(station_name or station_no)

    return {
        "next_run_at": f"{first_hour:02d}:{first_minute:02d}",
        "next_run_units": len(tied_slots),
        "next_run_stations": ",".join(item for item in station_labels if item),
    }


def _first_station_tz(cursor: Any, user_id: int) -> str:
    cursor.execute(
        """
        SELECT DISTINCT fp.timezone AS station_tz
        FROM patrol_capture_plan fp
        INNER JOIN patrol_station d
            ON fp.station_id = d.id AND d.status = '0' AND d.del_flag = '0'
        WHERE fp.user_id = %s AND fp.plan_state = '0'
          AND fp.timezone IS NOT NULL AND fp.timezone != ''
        LIMIT 1
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    return str(row.get("station_tz") or "") if row else ""


def _is_entry_class_only(cursor: Any, user_id: int) -> bool:
    cursor.execute(
        """
        SELECT p.model_code
        FROM patrol_station d
        LEFT JOIN patrol_station_model p ON d.model_ref = p.model_ref
        WHERE d.user_id = %s AND d.status = '0' AND d.del_flag = '0'
        """,
        (user_id,),
    )
    rows = cursor.fetchall()
    if not rows:
        return False
    return all(str(row.get("model_code") or "").upper() == ENTRY_MODEL for row in rows)


def _gather_snapshot(
    options: Options,
    *,
    user_id: int,
    user_uuid: str,
    user_language: str,
    user_timezone: str,
    station_tz: str,
) -> dict[str, Any]:
    today_start, today_end = _day_window(user_timezone)

    conn = open_ledger(options)
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COALESCE(d.station_no, '') AS station_no,
                    COALESCE(d.station_name, '') AS station_name,
                    COALESCE(p.model_code, '') AS model_code,
                    CONCAT(LPAD(fp.run_hour, 2, '0'), ':', LPAD(fp.run_minute, 2, '0')) AS run_slot,
                    COALESCE(fp.weekday_mask, '') AS weekday_mask,
                    fp.run_units,
                    COALESCE(fp.plan_state, '') AS plan_state
                FROM patrol_capture_plan fp
                INNER JOIN patrol_station d
                    ON fp.station_id = d.id AND d.status = '0' AND d.del_flag = '0'
                LEFT JOIN patrol_station_model p ON d.model_ref = p.model_ref
                WHERE fp.user_id = %s
                ORDER BY d.station_no, fp.run_hour, fp.run_minute
                """,
                (user_id,),
            )
            capture_plan = _normalize_rows(cursor.fetchall())

            cursor.execute(
                """
                SELECT
                    COALESCE(d.station_no, '') AS station_no,
                    COALESCE(d.station_name, '') AS station_name,
                    COALESCE(p.model_code, '') AS model_code,
                    fp.run_hour, fp.run_minute, fp.run_units,
                    COALESCE(fp.weekday_mask, '') AS weekday_mask,
                    fp.cadence
                FROM patrol_capture_plan fp
                INNER JOIN patrol_station d
                    ON fp.station_id = d.id AND d.status = '0' AND d.del_flag = '0'
                LEFT JOIN patrol_station_model p ON d.model_ref = p.model_ref
                WHERE fp.user_id = %s AND fp.plan_state = '0'
                """,
                (user_id,),
            )
            plan_preview = _next_window_preview(cursor.fetchall(), user_timezone)

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS capture_total,
                    COALESCE(SUM(CASE WHEN fr.trigger_kind = '0' THEN 1 ELSE 0 END), 0) AS manual_count,
                    COALESCE(SUM(CASE WHEN fr.trigger_kind = '1' THEN 1 ELSE 0 END), 0) AS auto_count,
                    COUNT(DISTINCT fr.station_id) AS station_total,
                    COALESCE(SUM(fr.run_units) * 8, 0) AS captured_units
                FROM patrol_capture_log fr
                WHERE fr.user_id = %s
                  AND fr.captured_at >= %s
                  AND fr.captured_at < %s
                """,
                (user_id, today_start, today_end),
            )
            capture_stats = _normalize_row(cursor.fetchone())

            cursor.execute(
                """
                SELECT
                    COALESCE(d.station_no, '') AS station_no,
                    COALESCE(d.station_name, '') AS station_name,
                    COALESCE(p.model_code, '') AS model_code,
                    DATE_FORMAT(fr.captured_at, '%%m-%%d %%H:%%i') AS captured_slot,
                    fr.run_units,
                    CASE fr.trigger_kind WHEN '0' THEN 'Manual' WHEN '1' THEN 'Auto' ELSE 'Unknown' END AS trigger_label
                FROM patrol_capture_log fr
                LEFT JOIN patrol_station d
                    ON fr.station_id = d.id AND d.user_id = fr.user_id
                LEFT JOIN patrol_station_model p ON d.model_ref = p.model_ref
                WHERE fr.user_id = %s
                  AND fr.captured_at >= %s
                  AND fr.captured_at < %s
                ORDER BY d.station_no, fr.captured_at ASC
                """,
                (user_id, today_start, today_end),
            )
            capture_log = _normalize_rows(cursor.fetchall())

            entry_class_only = _is_entry_class_only(cursor, user_id)
            if entry_class_only:
                rule_info = {
                    "event_total": 0,
                    "rule_state": 0,
                    "action_total": 0,
                    "object_total": 0,
                }
            else:
                cursor.execute(
                    """
                    SELECT s.rule_code, s.rule_name
                    FROM patrol_user_rule us
                    INNER JOIN patrol_rule s ON us.rule_ref = s.id AND s.status = '0'
                    WHERE us.user_id = %s AND us.status = '0'
                      AND s.rule_code IN ('200001', '200002')
                    """,
                    (user_id,),
                )
                rule_rows = cursor.fetchall()
                enabled_rule_codes = {str(row.get("rule_code") or "") for row in rule_rows}

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS event_total,
                        COALESCE(SUM(CASE WHEN m.capture_kind IN ('action', '2', '[2]') THEN 1 ELSE 0 END), 0) AS action_total,
                        COALESCE(SUM(CASE WHEN m.capture_kind IN ('object', '1', '[1]') THEN 1 ELSE 0 END), 0) AS object_total
                    FROM patrol_station_event m
                    WHERE m.user_id = %s
                      AND m.event_at >= %s
                      AND m.event_at < %s
                    """,
                    (user_id, today_start, today_end),
                )
                rule_info = _normalize_row(cursor.fetchone())
                enabled_rule_codes = sorted(enabled_rule_codes)
                rule_info["rule_state"] = enabled_rule_codes if enabled_rule_codes else 0

            cursor.execute(
                """
                SELECT
                    CONCAT('evt_', m.id) AS event_ref,
                    COALESCE(m.station_name, '') AS station_name,
                    COALESCE(m.station_no, '') AS station_no,
                    COALESCE(p.model_code, '') AS model_code,
                    COALESCE(m.headline, '') AS title,
                    m.capture_kind,
                    COALESCE(m.detail, '') AS body,
                    COALESCE(m.media_hint, '') AS media_hint
                FROM patrol_station_event m
                LEFT JOIN patrol_station d
                    ON m.station_no = d.station_no AND d.user_id = m.user_id
                LEFT JOIN patrol_station_model p ON d.model_ref = p.model_ref
                WHERE m.user_id = %s
                  AND m.event_at >= %s
                  AND m.event_at < %s
                ORDER BY m.event_at ASC
                """,
                (user_id, today_start, today_end),
            )
            event_log = _normalize_rows(cursor.fetchall())
            for event in event_log:
                event["capture_kind"] = _capture_kind_code(event.get("capture_kind"))

            station_tz = station_tz or _first_station_tz(cursor, user_id)

    return {
        "user_language": user_language,
        "user_timezone": user_timezone,
        "station_tz": station_tz,
        "plan_preview": plan_preview,
        "capture_plan": capture_plan,
        "capture_stats": capture_stats,
        "capture_log": capture_log,
        "rule_info": rule_info,
        "event_log": event_log,
    }


def _parse_json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed
    return None


def _coerce_outputs(outputs: dict[str, Any]) -> dict[str, Any]:
    if any(
        key in outputs
        for key in ("title", "captured_at", "period_body", "rule_body", "next_day_note")
    ):
        return outputs
    for key in ("result", "answer", "text", "output"):
        parsed = _parse_json_object(outputs.get(key))
        if parsed is not None:
            return parsed
    return outputs


def _pick_outputs(response_json: dict[str, Any]) -> dict[str, Any]:
    data = response_json.get("data")
    if isinstance(data, dict) and isinstance(data.get("outputs"), dict):
        return _coerce_outputs(data["outputs"])
    if isinstance(response_json.get("outputs"), dict):
        return _coerce_outputs(response_json["outputs"])

    for key in ("result", "answer", "text", "output"):
        parsed = _parse_json_object(response_json.get(key))
        if parsed is not None:
            return parsed

    return response_json


def _invoke_digest(
    options: Options,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if not options.digest_workflow_urls:
        raise RuntimeError("Missing DIGEST_WORKFLOW_URLS for period-digest workflow")
    return run_digest_workflow(options, payload)


def _digest_state(options: Options, digest_ref: str) -> str:
    conn = open_ledger(options)
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT job_state FROM patrol_digest_record WHERE digest_ref = %s LIMIT 1",
                (digest_ref,),
            )
            row = cursor.fetchone()
    return str(row.get("job_state") or "") if row else ""


def _write_digest_result(options: Options, digest_ref: str, data: dict[str, Any], outputs: dict[str, Any]) -> None:
    conn = open_ledger(options)
    with conn:
        with conn.cursor() as cursor:
            affected = cursor.execute(
                """
                UPDATE patrol_digest_record SET
                    job_state = '2',
                    user_language = %s,
                    user_timezone = %s,
                    station_tz = %s,
                    title = %s,
                    captured_at = %s,
                    period_body = %s,
                    rule_body = %s,
                    next_day_note = %s,
                    ai_suggestions = %s,
                    rule_text = %s,
                    plan_preview_json = %s,
                    capture_plan_json = %s,
                    capture_stats_json = %s,
                    capture_log_json = %s,
                    rule_info_json = %s,
                    event_log_json = %s,
                    update_time = NOW()
                WHERE digest_ref = %s
                """,
                (
                    data["user_language"],
                    data["user_timezone"],
                    data["station_tz"],
                    str(outputs.get("title") or ""),
                    str(outputs.get("captured_at") or ""),
                    str(outputs.get("period_body") or ""),
                    str(outputs.get("rule_body") or ""),
                    str(outputs.get("next_day_note") or ""),
                    str(outputs.get("ai_suggestions") or ""),
                    str(outputs.get("rule_text") or ""),
                    _json_dumps(data["plan_preview"]),
                    _json_dumps(data["capture_plan"]),
                    _json_dumps(data["capture_stats"]),
                    _json_dumps(data["capture_log"]),
                    _json_dumps(data["rule_info"]),
                    _json_dumps(data["event_log"]),
                    digest_ref,
                ),
            )
            if affected == 0:
                raise RuntimeError(f"patrol_digest_record update missed digest_ref={digest_ref}")


def _persist_gathered(options: Options, digest_ref: str, data: dict[str, Any]) -> None:
    """Persist collected DB snapshots before Workflow so collection results are visible even if Workflow fails."""
    conn = open_ledger(options)
    with conn:
        with conn.cursor() as cursor:
            affected = cursor.execute(
                """
                UPDATE patrol_digest_record SET
                    job_state = '1',
                    user_language = %s,
                    user_timezone = %s,
                    station_tz = %s,
                    plan_preview_json = %s,
                    capture_plan_json = %s,
                    capture_stats_json = %s,
                    capture_log_json = %s,
                    rule_info_json = %s,
                    event_log_json = %s,
                    remark = 'gathered_waiting_workflow',
                    update_time = NOW()
                WHERE digest_ref = %s
                """,
                (
                    data["user_language"],
                    data["user_timezone"],
                    data["station_tz"],
                    _json_dumps(data["plan_preview"]),
                    _json_dumps(data["capture_plan"]),
                    _json_dumps(data["capture_stats"]),
                    _json_dumps(data["capture_log"]),
                    _json_dumps(data["rule_info"]),
                    _json_dumps(data["event_log"]),
                    digest_ref,
                ),
            )
            if affected == 0:
                raise RuntimeError(f"patrol_digest_record snapshot update missed digest_ref={digest_ref}")
    logger.info(
        "digest snapshot persisted digest_ref=%s schedule_json=%d capture_plan_json=%d "
        "capture_stats_json=%d capture_log_json=%d rule_info_json=%d event_log_json=%d",
        digest_ref,
        len(_json_dumps(data["plan_preview"])),
        len(_json_dumps(data["capture_plan"])),
        len(_json_dumps(data["capture_stats"])),
        len(_json_dumps(data["capture_log"])),
        len(_json_dumps(data["rule_info"])),
        len(_json_dumps(data["event_log"])),
    )


def handle(job: JobEnvelope, options: Options) -> Outcome:
    if job.job_kind != JOB_KIND:
        raise ValueError(f"Unsupported job_kind for the period_digest handler: {job.job_kind}")

    digest_ref = _job_field(job, "digest_ref")
    if not digest_ref:
        raise ValueError("digest.compose job missing digest_ref")

    user_id_raw = _job_field(job, "user_id")
    if not user_id_raw:
        raise ValueError("digest.compose job missing user_id")
    user_id = int(user_id_raw)

    user_uuid = job.user_uuid or _job_field(job, "user_uuid")
    user_timezone = job.user_timezone or _job_field(job, "user_timezone", "UTC")
    user_language = job.user_language or _job_field(job, "user_language")
    station_tz = _job_field(job, "station_tz")

    if _digest_state(options, digest_ref) == "2":
        return Outcome(
            accepted=True,
            handler="period_digest",
            summary=f"digest_ref={digest_ref} already completed",
        )

    logger.info(
        "digest start digest_ref=%s job_ref=%s user_id=%s user_uuid=%s timezone=%s",
        digest_ref,
        job.job_ref,
        user_id,
        user_uuid,
        user_timezone,
    )
    mark_digest_running(options, digest_ref)

    data = _gather_snapshot(
        options,
        user_id=user_id,
        user_uuid=user_uuid,
        user_language=user_language,
        user_timezone=user_timezone,
        station_tz=station_tz,
    )
    logger.info(
        "digest data gathered digest_ref=%s next_run_at=%s next_run_units=%s capture_plan=%d "
        "capture_log=%d event_log=%d station_tz=%s",
        digest_ref,
        data["plan_preview"].get("next_run_at"),
        data["plan_preview"].get("next_run_units"),
        len(data["capture_plan"]),
        len(data["capture_log"]),
        len(data["event_log"]),
        data["station_tz"],
    )
    _persist_gathered(options, digest_ref, data)

    payload = {
        "workflow_name": PIPELINE_LABEL,
        "digest_ref": digest_ref,
        "job_ref": job.job_ref,
        "user_uuid": user_uuid,
        "user_language": data["user_language"],
        "user_timezone": data["user_timezone"],
        "station_tz": data["station_tz"],
        # Workflow workflow variables are configured as string inputs; keep DB snapshots typed,
        # but serialize workflow payload fields to JSON strings.
        "plan_preview": _json_dumps(data["plan_preview"]),
        "capture_plan": _json_dumps(data["capture_plan"]),
        "capture_stats": _json_dumps(data["capture_stats"]),
        "capture_log": _json_dumps(data["capture_log"]),
        "rule_info": _json_dumps(data["rule_info"]),
        "event_log": _json_dumps(data["event_log"]),
    }
    logger.info("digest handing off to workflow platform digest_ref=%s workflow=%s", digest_ref, PIPELINE_LABEL)
    outputs = _invoke_digest(options, payload)
    _write_digest_result(options, digest_ref, data, outputs)

    logger.info(
        "digest completed digest_ref=%s user_id=%s events=%s",
        digest_ref,
        user_id,
        len(data["event_log"]),
    )
    return Outcome(
        accepted=True,
        handler="period_digest",
        summary=f"digest_ref={digest_ref} workflow={PIPELINE_LABEL}",
    )
