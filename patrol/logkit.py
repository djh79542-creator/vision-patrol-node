from __future__ import annotations

import logging
import multiprocessing
import os
import sys
from pathlib import Path

from loguru import logger


def _is_main_process() -> bool:
    """仅主进程写文件日志，避免 ProcessPool 子进程多进程抢写同一文件。"""
    parent = getattr(multiprocessing, "parent_process", None)
    if parent is None:
        return True
    try:
        return parent() is None
    except Exception:
        return True


def setup_logging() -> None:
    # spawn 出来的子进程 stdout 是块缓冲的，不强制行缓冲的话日志会挤在退出时才 flush。
    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    # 全部输出收敛到 loguru；redis-py、uvicorn 这些第三方库走标准库 logging，
    # 由下面的 InterceptHandler 转投过来。
    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level:<8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
        colorize=True,
        enqueue=False,   # 子进程里不用异步队列，直接同步写，避免进程退出前日志丢失
        diagnose=False,
    )

    log_dir = (os.getenv("LOG_DIR") or "").strip()
    if log_dir and _is_main_process():
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        logger.add(
            str(log_path / "patrol-node.log"),
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name} - {message}",
            rotation="50 MB",
            retention="14 days",
            encoding="utf-8",
            colorize=False,
            enqueue=True,
        )

    # 把标准库 logging 的日志拦截转发给 loguru，
    # 这样 redis-py、uvicorn、pymysql 等库的日志也经过统一格式输出。
    logging.basicConfig(handlers=[_InterceptHandler()], level=logging.INFO, force=True)

    # 压制第三方库的 INFO 级别噪音，只保留 WARNING 以上
    for noisy in ("httpx", "httpcore", "hpack"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


class _InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno  # type: ignore[assignment]

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back  # type: ignore[assignment]
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())
