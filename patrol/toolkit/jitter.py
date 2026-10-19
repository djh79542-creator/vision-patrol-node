from __future__ import annotations

import random


def jittered_delay(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """
    计算带 jitter 的指数退避等待时间。
    - attempt 从 0 开始
    - delay = min(base * 2^attempt, cap)
    - jitter ±10%，避免多实例同时重连的惊群效应
    """
    delay = min(base * (2 ** attempt), cap)
    jitter = random.uniform(-delay * 0.1, delay * 0.1)
    return max(0.0, delay + jitter)
