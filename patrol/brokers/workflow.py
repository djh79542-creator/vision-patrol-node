from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from patrol.options import Options

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class EndpointState:
    url: str
    failure_count: int = 0
    circuit_open_until: float = 0.0


class WorkflowGateway:
    """线程安全的摘要作业出口：多端点分流、在途限流、重试与熔断。"""

    def __init__(
        self,
        *,
        urls: tuple[str, ...],
        timeout_seconds: int,
        max_inflight: int,
        http_retries: int,
        balance: str,
        failure_threshold: int,
        circuit_cooldown_seconds: int,
    ) -> None:
        if not urls:
            raise ValueError("Workflow 端点列表不能为空")

        self._states = [EndpointState(url=url) for url in urls]
        self._timeout_seconds = max(1, timeout_seconds)
        self._max_inflight = max(1, max_inflight)
        self._http_retries = max(0, http_retries)
        self._balance = balance
        self._failure_threshold = max(1, failure_threshold)
        self._circuit_cooldown_seconds = max(1, circuit_cooldown_seconds)

        self._semaphore = threading.BoundedSemaphore(self._max_inflight)
        self._lock = threading.Lock()
        self._next_index = 0

    @property
    def signature(self) -> tuple[Any, ...]:
        return (
            tuple(state.url for state in self._states),
            self._timeout_seconds,
            self._max_inflight,
            self._http_retries,
            self._balance,
            self._failure_threshold,
            self._circuit_cooldown_seconds,
        )

    def call_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        acquired = self._semaphore.acquire(timeout=self._timeout_seconds)
        if not acquired:
            raise TimeoutError(
                f"Workflow inflight limit exceeded max_inflight={self._max_inflight}"
            )

        try:
            return self._call_with_retries(payload)
        finally:
            self._semaphore.release()

    def _call_with_retries(self, payload: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        attempts = self._http_retries + 1

        for attempt in range(1, attempts + 1):
            state = self._select_endpoint()
            try:
                response = httpx.post(
                    state.url,
                    json=payload,
                    timeout=self._timeout_seconds,
                )
                if response.status_code >= 400:
                    error = RuntimeError(
                        f"Workflow HTTP {response.status_code} url={state.url} body={response.text[:200]}"
                    )
                    self._record_failure(state, error)
                    last_error = error
                    if not self._retryable_status(response.status_code) or attempt >= attempts:
                        raise error
                    continue

                self._record_success(state)
                return self._extract_outputs(response)
            except (httpx.TimeoutException, httpx.RequestError) as exc:
                error = RuntimeError(f"Workflow request failed url={state.url} error={exc}")
                self._record_failure(state, error)
                last_error = error
                if attempt >= attempts:
                    raise error from exc

        raise last_error or RuntimeError("Workflow request failed")

    def _select_endpoint(self) -> EndpointState:
        now = time.monotonic()
        with self._lock:
            available_indexes = [
                idx for idx, state in enumerate(self._states)
                if state.circuit_open_until <= now
            ]
            if not available_indexes:
                soonest = min(state.circuit_open_until for state in self._states)
                wait_seconds = max(0.0, soonest - now)
                raise RuntimeError(
                    f"All Workflow endpoints are circuit-open wait_seconds={wait_seconds:.1f}"
                )

            if self._balance != "round_robin":
                return self._states[available_indexes[0]]

            total = len(self._states)
            for offset in range(total):
                idx = (self._next_index + offset) % total
                if idx in available_indexes:
                    self._next_index = (idx + 1) % total
                    return self._states[idx]

            return self._states[available_indexes[0]]

    def _record_success(self, state: EndpointState) -> None:
        with self._lock:
            state.failure_count = 0
            state.circuit_open_until = 0.0

    def _record_failure(self, state: EndpointState, error: Exception) -> None:
        with self._lock:
            state.failure_count += 1
            if state.failure_count >= self._failure_threshold:
                state.circuit_open_until = time.monotonic() + self._circuit_cooldown_seconds
                logger.warning(
                    "Workflow endpoint circuit opened url=%s failure_count=%d cooldown=%ds error=%s",
                    state.url,
                    state.failure_count,
                    self._circuit_cooldown_seconds,
                    error,
                )

    @staticmethod
    def _retryable_status(status_code: int) -> bool:
        return status_code >= 500 or status_code in {408, 429}

    def _extract_outputs(self, response: httpx.Response) -> dict[str, Any]:
        if not response.text.strip():
            return {}
        try:
            response_json = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Workflow returned non-json body: {response.text[:200]}") from exc

        if not isinstance(response_json, dict):
            raise RuntimeError(f"Workflow returned unexpected json type: {type(response_json).__name__}")

        data = response_json.get("data")
        if isinstance(data, dict) and isinstance(data.get("outputs"), dict):
            return self._coerce_outputs(data["outputs"])
        if isinstance(response_json.get("outputs"), dict):
            return self._coerce_outputs(response_json["outputs"])

        for key in ("result", "answer", "text", "output"):
            parsed = self._parse_json_object(response_json.get(key))
            if parsed is not None:
                return parsed

        return response_json

    def _coerce_outputs(self, outputs: dict[str, Any]) -> dict[str, Any]:
        if any(
            key in outputs
            for key in ("title", "captured_at", "period_body", "rule_body", "next_day_note")
        ):
            return outputs

        for key in ("result", "answer", "text", "output"):
            parsed = self._parse_json_object(outputs.get(key))
            if parsed is not None:
                return parsed
        return outputs

    @staticmethod
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


_GATEWAY: WorkflowGateway | None = None
_GATEWAY_STAMP: tuple[Any, ...] | None = None
_gateway_lock = threading.Lock()


def digest_gateway(options: Options) -> WorkflowGateway:
    global _GATEWAY, _GATEWAY_STAMP

    urls = options.digest_workflow_urls
    signature = (
        urls,
        options.digest_workflow_timeout_sec,
        options.digest_workflow_max_inflight,
        options.digest_workflow_retries,
        options.digest_workflow_balance,
        options.digest_workflow_failure_threshold,
        options.digest_workflow_cooldown_sec,
    )

    with _gateway_lock:
        if _GATEWAY is None or _GATEWAY_STAMP != signature:
            _GATEWAY = WorkflowGateway(
                urls=urls,
                timeout_seconds=options.digest_workflow_timeout_sec,
                max_inflight=options.digest_workflow_max_inflight,
                http_retries=options.digest_workflow_retries,
                balance=options.digest_workflow_balance,
                failure_threshold=options.digest_workflow_failure_threshold,
                circuit_cooldown_seconds=options.digest_workflow_cooldown_sec,
            )
            _GATEWAY_STAMP = signature
        return _GATEWAY


def run_digest_workflow(options: Options, payload: dict[str, Any]) -> dict[str, Any]:
    return digest_gateway(options).call_workflow(payload)
