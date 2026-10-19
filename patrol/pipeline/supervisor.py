from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any

from redis.exceptions import ResponseError

from patrol.options import Options
from patrol.messages.errors import EnvelopeError
from patrol.messages.envelope import JobEnvelope
from patrol.inspections.router import route_job
from patrol.brokers.streams import StreamHandle, open_stream, build_lease_key
from patrol.brokers.ledger import (
    mark_digest_dropped,
    mark_digest_queued,
    mark_handoff_pending,
    mark_finished,
    mark_dropped,
    mark_running,
)
from patrol.toolkit.jitter import jittered_delay
from patrol.pipeline.quarantine import drop_job, read_delivery_count
from patrol.pipeline.lease_keeper import LeaseKeeper
from patrol.pipeline.decode import decode_envelope

logger = logging.getLogger(__name__)
DIGEST_JOB_KIND = "digest.compose"


def _is_digest_job(job: JobEnvelope) -> bool:
    return job.job_kind == DIGEST_JOB_KIND


def _digest_ref(job: JobEnvelope) -> str | None:
    return job.raw.get("digest_ref")


# ------------------------------------------------------------------
# 模块级作业处理函数（ProcessPoolExecutor 要求可 pickle，不能是实例方法）
# 子进程内部自建 Redis 连接，与主进程完全隔离
# ------------------------------------------------------------------

def _run_one(
    options: Options,
    job: JobEnvelope,
    *,
    is_reclaim: bool,
    init_logging: bool = False,
    pool_label: str | None = None,
) -> None:
    """
    单条作业处理入口。
    - 进程模式（ProcessPoolExecutor）：init_logging=True，子进程需重新初始化日志和连接
    - 线程模式（ThreadPoolExecutor）：init_logging=False，共享主进程日志和环境
    """
    import os  # noqa: PLC0415
    import threading  # noqa: PLC0415

    if init_logging:
        from patrol.logkit import setup_logging  # noqa: PLC0415
        setup_logging()

    # 执行标识：进程模式显示 pid，线程模式显示线程序号
    run_mode = options.run_mode
    if pool_label:
        runner_tag = f"{pool_label}/{threading.current_thread().name}"
    elif run_mode == "process":
        runner_tag = f"process/pid={os.getpid()}"
    else:
        thread_name = threading.current_thread().name
        # ThreadPoolExecutor-0_3 → 取最后的序号 "3"
        thread_num = thread_name.rsplit("_", 1)[-1]
        runner_tag = f"thread/{thread_num}"

    stream_handle: StreamHandle = open_stream(options)
    group_id = options.stream_group
    member_id = options.member_name
    lease_prefix = options.lease_prefix
    lease_ttl_sec = options.lease_ttl_sec
    max_deliveries = options.max_deliveries
    max_lease_renewals = options.lease_max_renewals

    # 过期检查：首次和重试路径均拦截，不消耗重试次数
    if job.is_expired:
        logger.warning(
            "作业已过期，跳过处理 job_id=%s entry_id=%s expires_at=%s",
            job.job_id, job.entry_id, job.expires_at,
        )
        if _is_digest_job(job) and _digest_ref(job):
            mark_digest_dropped(options, _digest_ref(job) or "")
        else:
            mark_dropped(options, job.job_id)
        stream_handle.xack(job.topic, group_id, job.entry_id)
        stream_handle.xdel(job.topic, job.entry_id)
        return

    if is_reclaim:
        delivery_count = read_delivery_count(
            stream_handle, job.topic, group_id, job.entry_id,
        )
        if delivery_count > max_deliveries:
            drop_job(
                stream_handle,
                group_id,
                job.topic,
                job.entry_id,
                job.job_id,
                delivery_count,
                options,
                job_kind=job.job_kind,
                digest_ref=_digest_ref(job),
            )
            return

    lease_ref = build_lease_key(lease_prefix, job.topic, job.job_id)
    lease_held = stream_handle.set(
        name=lease_ref,
        value=member_id,
        ex=lease_ttl_sec,
        nx=True,
    )

    if not lease_held:
        logger.info(
            "跳过作业 topic=%s entry_id=%s job_id=%s reason=未抢到租约",
            job.topic, job.entry_id, job.job_id,
        )
        return

    keeper = LeaseKeeper(
        stream_handle=stream_handle,
        lease_ref=lease_ref,
        lease_ttl_sec=lease_ttl_sec,
        member_id=member_id,
        max_renewals=max_lease_renewals,
    )

    handled = False
    try:
        logger.info(
            "[%s] [%s] 开始下发作业 topic=%s entry_id=%s job_ref=%s digest_ref=%s job_kind=%s station_id=%s",
            runner_tag, group_id,
            job.topic, job.entry_id, job.job_id, _digest_ref(job) or "n/a",
            job.job_kind, job.station_id or "n/a",
        )
        if not _is_digest_job(job):
            mark_running(options, job.job_id, member_id)

        with keeper.watch():
            result = route_job(job, options)

        # Workflow 路径：标记为 Workflow 处理中（status=4），状态由 Workflow 回调写最终结果
        if _is_digest_job(job):
            pass
        elif result.handler != "workflow":
            mark_finished(options, job.job_id, result.accepted)
        else:
            mark_handoff_pending(options, job.job_id)
        handled = True

        logger.info(
            "[%s] [%s] 作业已完成 topic=%s entry_id=%s job_id=%s handler=%s accepted=%s summary=%s",
            runner_tag, group_id,
            job.topic, job.entry_id, job.job_id,
            result.handler, result.accepted, result.summary,
        )
        stream_handle.xack(job.topic, group_id, job.entry_id)
        stream_handle.xdel(job.topic, job.entry_id)
        stream_handle.delete(lease_ref)

    except Exception as error:  # noqa: BLE001
        logger.exception(
            "[%s] [%s] 作业处理失败，留在 pending 等待重试 topic=%s entry_id=%s job_ref=%s digest_ref=%s error=%s",
            runner_tag, group_id,
            job.topic, job.entry_id, job.job_id, _digest_ref(job) or "n/a", error,
        )
        if _is_digest_job(job) and _digest_ref(job) and not handled:
            mark_digest_queued(options, _digest_ref(job) or "", str(error))
        # Release lease proactively so another node can retry after pending idle.
        try:
            stream_handle.delete(lease_ref)
        except Exception:
            pass



class JobSupervisor:
    """
    Redis Stream member.

    Two executor pools are intentionally isolated:
    - default pool: existing Vision / Workflow forwarding path, type controlled by PIPELINE_RUN_MODE
    - period-digest pool: always ThreadPoolExecutor, controlled by DIGEST_WORKERS
    """

    def __init__(self, options: Options) -> None:
        self.options = options
        self.topics = options.job_topics
        self.group_id = options.stream_group
        self.member_id = options.member_name
        self.block_window_ms = options.poll_block_ms
        self.stale_after_ms = options.stale_after_ms
        self.concurrency = max(1, options.pipeline_workers)
        self.digest_workers = max(1, options.digest_workers)
        self.poll_count = max(1, options.poll_count)
        self.run_mode = options.run_mode

        self.ops_handle: StreamHandle = open_stream(options)
        self.wait_handle: StreamHandle = open_stream(options)
        self._stop_event = threading.Event()
        self._main_pool: ProcessPoolExecutor | ThreadPoolExecutor | None = None
        self._digest_pool: ThreadPoolExecutor | None = None
        self._main_bucket: dict[Future[None], JobEnvelope] = {}
        self._digest_bucket: dict[Future[None], JobEnvelope] = {}
        self._rotate_index = 0
        self._io_shut = False
        self._shutdown_guard = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        self.ops_handle.ping()
        self.wait_handle.ping()
        self._ensure_groups()
        self._stop_event.clear()

        self._main_pool = self._build_main_pool()
        self._digest_pool = ThreadPoolExecutor(
            max_workers=self.digest_workers,
            thread_name_prefix="period-digest",
        )
        logger.info(
            "JobSupervisor started mode=%s default_executor=%s streams=%s group=%s member=%s "
            "default_concurrency=%d digest_workers=%d poll_count=%d max_deliveries=%d max_lease_renewals=%s",
            self.options.stream_mode,
            self.run_mode,
            ",".join(self.topics),
            self.group_id,
            self.member_id,
            self.concurrency,
            self.digest_workers,
            self.poll_count,
            self.options.max_deliveries,
            self.options.lease_max_renewals or "unlimited",
        )

        try:
            self._poll_loop()
        finally:
            self._close_pools()

    def stop(self) -> None:
        self._stop_event.set()
        with self._shutdown_guard:
            if self._io_shut:
                return
            self._io_shut = True
        try:
            self.wait_handle.close()
        except Exception:
            pass
        try:
            self.ops_handle.close()
        except Exception:
            pass
        logger.info("JobSupervisor stopped")

    def _close_pools(self) -> None:
        if self._main_pool is not None:
            self._main_pool.shutdown(wait=True, cancel_futures=True)
            self._main_pool = None
        if self._digest_pool is not None:
            self._digest_pool.shutdown(wait=True, cancel_futures=True)
            self._digest_pool = None

    # ------------------------------------------------------------------
    # Executors / capacity
    # ------------------------------------------------------------------

    def _build_main_pool(self) -> ProcessPoolExecutor | ThreadPoolExecutor:
        if self.run_mode == "process":
            return ProcessPoolExecutor(max_workers=self.concurrency)
        return ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix="runner-default")

    def _stream_is_digest(self, topic: str) -> bool:
        return topic == self.options.digest_topic

    def _bucket_for_job(self, job: JobEnvelope) -> dict[Future[None], JobEnvelope]:
        if _is_digest_job(job):
            return self._digest_bucket
        return self._main_bucket

    def _bucket_for_stream(self, topic: str) -> dict[Future[None], JobEnvelope]:
        if self._stream_is_digest(topic):
            return self._digest_bucket
        return self._main_bucket

    def _cap_for_job(self, job: JobEnvelope) -> int:
        return self.digest_workers if _is_digest_job(job) else self.concurrency

    def _cap_for_stream(self, topic: str) -> int:
        return self.digest_workers if self._stream_is_digest(topic) else self.concurrency

    def _free_slots(self, topic: str) -> int:
        future_map = self._bucket_for_stream(topic)
        return max(0, self._cap_for_stream(topic) - len(future_map))

    def _has_slot(self) -> bool:
        return any(self._free_slots(topic) > 0 for topic in self.topics)

    def _rotated_topics(self) -> list[str]:
        streams = list(self.topics)
        if not streams:
            return []
        start = self._rotate_index % len(streams)
        self._rotate_index += 1
        return streams[start:] + streams[:start]

    def _reap_finished(self) -> None:
        self._reap_bucket(self._main_bucket, "default")
        self._reap_bucket(self._digest_bucket, "period-digest")

    def _reap_bucket(self, futures: dict[Future[None], JobEnvelope], pool_name: str) -> None:
        done = [future for future in futures if future.done()]
        for future in done:
            job = futures.pop(future)
            try:
                future.result()
            except Exception:
                logger.exception(
                    "Unhandled job future exception pool=%s job_id=%s topic=%s entry_id=%s",
                    pool_name,
                    job.job_id,
                    job.topic,
                    job.entry_id,
                )

    # ------------------------------------------------------------------
    # Redis member group
    # ------------------------------------------------------------------

    def _ensure_groups(self) -> None:
        for topic in self.topics:
            try:
                self.ops_handle.xgroup_create(
                    name=topic,
                    groupname=self.group_id,
                    id="0",
                    mkstream=True,
                )
                logger.info("created member group topic=%s group=%s", topic, self.group_id)
            except ResponseError as error:
                if "BUSYGROUP" not in str(error):
                    raise

    def _poll_loop(self) -> None:
        error_count = 0

        while not self._stop_event.is_set():
            try:
                self._reap_finished()

                if not self._has_slot():
                    self._stop_event.wait(0.1)
                    continue

                had_claimed = False
                for topic in self._rotated_topics():
                    capacity = self._free_slots(topic)
                    if capacity <= 0:
                        continue
                    claimed = self._claim_stale(topic, count=min(self.poll_count, capacity))
                    if claimed:
                        had_claimed = True
                        self._submit_batch(claimed, is_reclaim=True)

                if had_claimed:
                    error_count = 0
                    continue

                had_new = False
                for topic in self._rotated_topics():
                    if self._stop_event.is_set():
                        return

                    capacity = self._free_slots(topic)
                    if capacity <= 0:
                        continue

                    response = self.wait_handle.xreadgroup(
                        groupname=self.group_id,
                        consumername=self.member_id,
                        streams={topic: ">"},
                        count=min(self.poll_count, capacity),
                        block=self.block_window_ms,
                    )

                    jobs: list[JobEnvelope] = []
                    for _, entries in (response or []):
                        for entry_id, fields in entries:
                            jobs.append(decode_envelope(topic, entry_id, fields))

                    if jobs:
                        had_new = True
                        self._submit_batch(jobs, is_reclaim=False)
                        break

                if had_new:
                    error_count = 0

            except EnvelopeError as error:
                logger.warning("invalid job skipped: %s", error)
                error_count = 0
            except Exception as error:  # noqa: BLE001
                error_count += 1
                delay = jittered_delay(attempt=error_count - 1)
                logger.exception(
                    "member loop error attempt=%d delay=%.1fs error=%s",
                    error_count,
                    delay,
                    error,
                )
                self._stop_event.wait(delay)

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _submit_batch(self, jobs: list[JobEnvelope], *, is_reclaim: bool) -> None:
        if not self._main_pool or not self._digest_pool:
            return

        for job in jobs:
            future_map = self._bucket_for_job(job)
            limit = self._cap_for_job(job)
            if len(future_map) >= limit:
                logger.warning(
                    "executor saturated; job remains pending topic=%s entry_id=%s job_id=%s job_kind=%s limit=%d",
                    job.topic,
                    job.entry_id,
                    job.job_id,
                    job.job_kind,
                    limit,
                )
                continue

            is_digest = _is_digest_job(job)
            executor = self._digest_pool if is_digest else self._main_pool
            init_logging = False if is_digest else self.run_mode == "process"
            future = executor.submit(
                _run_one,
                self.options,
                job,
                is_reclaim=is_reclaim,
                init_logging=init_logging,
                pool_label="digest-thread" if is_digest else None,
            )
            future_map[future] = job

    def _claim_stale(self, topic: str, *, count: int) -> list[JobEnvelope]:
        response = self.ops_handle.xautoclaim(
            name=topic,
            groupname=self.group_id,
            consumername=self.member_id,
            min_idle_time=self.stale_after_ms,
            start_id="0-0",
            count=count,
        )

        jobs: list[JobEnvelope] = []
        for entry_id, fields in self._unpack_claimed(response):
            try:
                jobs.append(decode_envelope(topic, entry_id, fields))
            except EnvelopeError as error:
                logger.warning("invalid pending job skipped topic=%s error=%s", topic, error)
        return jobs

    def _unpack_claimed(self, response: Any) -> list[tuple[str, dict[str, str]]]:
        if not response:
            return []
        if isinstance(response, (tuple, list)):
            if len(response) >= 2 and isinstance(response[1], list):
                return response[1]
        return []
