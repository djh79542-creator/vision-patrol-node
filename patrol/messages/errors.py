from __future__ import annotations


class EnvelopeError(ValueError):
    """作业字段校验失败时抛出，消费循环捕获后跳过该条消息。"""
