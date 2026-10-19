from __future__ import annotations

import logging
import os
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

_DESCRIPTOR_URL = (
    "https://raw.githubusercontent.com/Qengineering/"
    "MobileNetV1_SSD_OpenCV_Caffe/master/MobileNetSSD_deploy.prototxt"
)
_WEIGHTS_URL = (
    "https://raw.githubusercontent.com/Qengineering/"
    "MobileNetV1_SSD_OpenCV_Caffe/master/MobileNetSSD_deploy.caffemodel"
)

DESCRIPTOR_FILE = "MobileNetSSD_deploy.prototxt"
WEIGHTS_FILE = "MobileNetSSD_deploy.caffemodel"


def ensure_bundle(bundle_dir: str) -> tuple[str, str]:
    """
    确保 MobileNet SSD 模型文件存在，返回 (proto_path, model_path)。
    文件缺失时自动从 GitHub 下载。
    """
    Path(bundle_dir).mkdir(parents=True, exist_ok=True)
    proto_path = str(Path(bundle_dir) / DESCRIPTOR_FILE)
    model_path = str(Path(bundle_dir) / WEIGHTS_FILE)

    _ensure_file(proto_path, _DESCRIPTOR_URL, "prototxt")
    _ensure_file(model_path, _WEIGHTS_URL, "caffemodel")

    return proto_path, model_path


def _ensure_file(dest: str, url: str, label: str) -> None:
    if Path(dest).exists() and Path(dest).stat().st_size > 0:
        return

    logger.info("权重文件缺失，开始下载 %s -> %s", label, dest)
    tmp = dest + ".tmp"
    try:
        urllib.request.urlretrieve(url, tmp)
        if not Path(tmp).exists() or Path(tmp).stat().st_size == 0:
            raise RuntimeError("下载后文件为空")
        os.replace(tmp, dest)
        logger.info("权重下载完成 %s (%d bytes)", label, Path(dest).stat().st_size)
    except Exception as exc:
        Path(tmp).unlink(missing_ok=True)
        raise RuntimeError(f"模型文件下载失败 {label}: {exc}") from exc
