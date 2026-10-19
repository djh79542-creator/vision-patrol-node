from __future__ import annotations

from typing import Any

from redis import Redis
from redis.cluster import ClusterNode, RedisCluster

from patrol.options import Options

StreamHandle = Any


def open_stream(options: Options) -> StreamHandle:
    """根据 STREAM_MODE 构建对应的 Redis 客户端。decode_responses=True 统一返回字符串。"""
    if options.stream_mode == "cluster":
        startup_nodes = [
            ClusterNode(host=host, port=port)
            for host, port in options.stream_cluster_nodes
        ]
        return RedisCluster(
            startup_nodes=startup_nodes,
            username=options.stream_user,
            password=options.stream_token,
            decode_responses=True,
        )

    if options.stream_host is None or options.stream_port is None or options.stream_db is None:
        raise ValueError("standalone 模式下 STREAM_HOST / STREAM_PORT / STREAM_DB 必须配置")

    return Redis(
        host=options.stream_host,
        port=options.stream_port,
        username=options.stream_user,
        password=options.stream_token,
        db=options.stream_db,
        decode_responses=True,
    )


def extract_shard_tag(key: str) -> str | None:
    """提取 Redis Cluster hash tag，用于保证 topic key 和 lease key 落在同一 slot。"""
    start = key.find("{")
    end = key.find("}", start + 1)
    if start == -1 or end == -1 or end <= start + 1:
        return None
    return key[start + 1 : end]


def build_lease_key(lease_prefix: str, topic: str, job_id: str) -> str:
    """构建作业租约 key；cluster 模式下带 hash tag 保证同 slot。"""
    normalized_prefix = lease_prefix.rstrip(":")
    hash_tag = extract_shard_tag(topic)
    if hash_tag:
        return f"{normalized_prefix}:{{{hash_tag}}}:{job_id}"
    return f"{normalized_prefix}:{job_id}"
