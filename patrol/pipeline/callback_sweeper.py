from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

import pymysql
import pymysql.cursors

if TYPE_CHECKING:
    from patrol.options import Options

logger = logging.getLogger(__name__)

JOB_TABLE = "patrol_job_ledger"


class CallbackSweeper:
    """
    Workflow 回调补偿扫描器。

    定期扫描 MySQL 中 status=4（Workflow 处理中）且超过超时时间未更新的作业，
    将其标记为 status=9（处理失败），防止作业永久卡在 Workflow 处理中状态。

    适用场景：
    - Workflow webhook 调用失败，回调方收不到通知
    - 回调方服务宕机，MySQL 状态未被回写
    - 网络抖动导致回调丢失
    """

    def __init__(self, options: Options) -> None:
        self.options = options
        self.timeout_minutes = options.sweep_stale_minutes
        self.scan_interval_seconds = options.sweep_interval_sec
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="workflow-keeper",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "CallbackSweeper 已启动 timeout=%dmin scan_interval=%ds",
            self.timeout_minutes,
            self.scan_interval_seconds,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("CallbackSweeper 已停止")

    def _run(self) -> None:
        while not self._stop_event.wait(self.scan_interval_seconds):
            try:
                self._scan_and_fix()
            except Exception:
                logger.exception("CallbackSweeper 扫描异常，下轮继续")

    def _scan_and_fix(self) -> None:
        try:
            conn = pymysql.connect(
                host=self.options.ledger_host,
                port=self.options.ledger_port,
                user=self.options.ledger_user,
                password=self.options.ledger_token,
                database=self.options.ledger_schema,
                charset="utf8mb4",
                connect_timeout=5,
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor,
            )
        except Exception:
            logger.exception("CallbackSweeper MySQL 连接失败，跳过本轮扫描")
            return

        with conn:
            with conn.cursor() as cur:
                # 查询 status=4 且超过超时时间未更新的作业
                cur.execute(
                    f"""
                    SELECT job_ref, update_time
                    FROM   {JOB_TABLE}
                    WHERE  status = '4'
                    AND    update_time < NOW() - INTERVAL %s MINUTE
                    LIMIT  100
                    """,
                    (self.timeout_minutes,),
                )
                rows = cur.fetchall()

            if not rows:
                return

            logger.warning(
                "CallbackSweeper 发现 %d 条 Workflow 回调超时作业（超过 %d 分钟未更新）",
                len(rows), self.timeout_minutes,
            )

            job_refs = [row["job_ref"] for row in rows]
            error_msg = f"Workflow 回调超时（超过 {self.timeout_minutes} 分钟未收到回调），标记为处理失败"

            with conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(job_refs))
                affected = cur.execute(
                    f"""
                    UPDATE {JOB_TABLE}
                    SET    status       = '9',
                           failure_note = %s,
                           update_time  = NOW()
                    WHERE  job_ref IN ({fmt})
                    AND    status = '4'
                    """,
                    (error_msg, *job_refs),
                )

            logger.warning(
                "CallbackSweeper 已将 %d 条超时作业标记为 status=9 job_refs=%s",
                affected, job_refs,
            )
