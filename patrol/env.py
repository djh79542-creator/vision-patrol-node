from __future__ import annotations

import os
import socket


def read_env(key: str) -> str | None:
    value = os.getenv(key)
    return value.strip() if value and value.strip() else None


def require_env(key: str) -> str:
    value = read_env(key)
    if value is None:
        raise ValueError(f"缺少必填配置: {key}")
    return value


def require_int(key: str) -> int:
    value = require_env(key)
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"配置 {key} 不是合法整数: {value}") from error


def optional_int(key: str, default: int) -> int:
    value = read_env(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"配置 {key} 不是合法整数: {value}") from error


def optional_bool(key: str, *, default: bool) -> bool:
    """读取布尔型环境变量，接受 true/false/1/0/yes/no（大小写不敏感）。"""
    value = read_env(key)
    if value is None:
        return default
    lower = value.lower()
    if lower in {"true", "1", "yes"}:
        return True
    if lower in {"false", "0", "no"}:
        return False
    raise ValueError(f"配置 {key} 不是合法布尔值: {value}，允许 true/false/1/0/yes/no")


def resolve_member_name() -> str:
    explicit = read_env("STREAM_MEMBER")
    if explicit:
        return explicit
    # 未配置时回退：hostname + pid，保证多实例唯一性
    return f"{socket.gethostname()}-{os.getpid()}"
