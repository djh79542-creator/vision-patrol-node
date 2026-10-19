from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Generator

logger = logging.getLogger(__name__)


class LeaseKeeper:
    """
    在作业执行期间后台每隔 TTL/3 秒续期租约。
    防止 Vision 推理时间超过租约 TTL 导致租约提前过期、其他实例重复接管作业。

    max_renewals > 0 时：续期达到上限后停止续租，租约在 TTL 秒后自然到期。
    适用于进程池模式：无法强杀线程，但可通过停续租让其他实例接管（类似 Celery soft_limit）。
    max_renewals = 0（默认）：不限次数，作业跑多久续多久。
    """

    def __init__(
        self,
        stream_handle: object,
        lease_ref: str,
        lease_ttl_sec: int,
        member_id: str,
        max_renewals: int = 0,
    ) -> None:
        self._client = stream_handle
        self._lease_ref = lease_ref
        self._lease_ttl_sec = lease_ttl_sec
        self._member_id = member_id
        self._max_renewals = max_renewals
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._renew_loop, daemon=True, name="lease-keeper")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _renew_loop(self) -> None:
        interval = max(self._lease_ttl_sec / 3, 1.0)
        renewal_count = 0

        while not self._stop_event.wait(interval):
            # 达到上限：停止续租，租约在 TTL 秒后自然到期（类似 soft timeout）
            if self._max_renewals > 0 and renewal_count >= self._max_renewals:
                logger.warning(
                    "租约续期已达上限（%d 次），停止续租 key=%s "
                    "作业将在 %ds 内因租约过期被其他实例接管",
                    self._max_renewals, self._lease_ref, self._lease_ttl_sec,
                )
                return

            try:
                current_owner = self._client.get(self._lease_ref)
                if current_owner != self._member_id:
                    logger.warning(
                        "租约续期终止：租约已不属于本成员 key=%s expected=%s actual=%s",
                        self._lease_ref,
                        self._member_id,
                        current_owner,
                    )
                    return
                self._client.expire(self._lease_ref, self._lease_ttl_sec)
                renewal_count += 1
                logger.debug(
                    "租约已续期 key=%s ttl=%ds count=%d/%s",
                    self._lease_ref, self._lease_ttl_sec,
                    renewal_count, self._max_renewals or "∞",
                )
            except Exception:
                logger.exception("租约续期异常，停止续租 key=%s", self._lease_ref)
                return

    @contextmanager
    def watch(self) -> Generator[None, None, None]:
        self.start()
        try:
            yield
        finally:
            self.stop()
