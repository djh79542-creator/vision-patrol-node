from __future__ import annotations

import logging
import tempfile
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)


def acquire_asset(asset_path: str | None, asset_uri: str | None) -> tuple[str, bool]:
    """
    确保图片在本地可用，返回 (local_path, is_temp)。

    - 若 asset_path 非空且文件存在，直接返回，is_temp=False
    - 若 asset_uri 非空，下载到临时文件，is_temp=True（调用方负责删除）
    - 两者都为空则抛出 ValueError
    """
    if asset_path and Path(asset_path).exists():
        return asset_path, False

    if asset_uri:
        suffix = _suffix_from_url(asset_uri)
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        tmp.close()
        logger.debug("下载图片 url=%s -> %s", asset_uri, tmp.name)
        try:
            urllib.request.urlretrieve(asset_uri, tmp.name)
        except Exception as exc:
            Path(tmp.name).unlink(missing_ok=True)
            raise RuntimeError(f"画面下载失败 url={asset_uri}: {exc}") from exc
        return tmp.name, True

    raise ValueError("asset_path 和 asset_uri 均为空，无法获取图片")


def drop_temp(path: str, is_temp: bool) -> None:
    """删除临时文件（is_temp=False 时什么都不做）。"""
    if is_temp:
        Path(path).unlink(missing_ok=True)


def _suffix_from_url(url: str) -> str:
    lower = url.lower().split("?")[0]
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".bmp"):
        if lower.endswith(ext):
            return ext
    return ".jpg"
