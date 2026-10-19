from __future__ import annotations

import threading
import cv2
from dataclasses import dataclass


@dataclass
class ScanHit:
    label: str             # "cat" | "dog"
    confidence: float      # 0.0-1.0
    x: int
    y: int
    w: int
    h: int


_LABELS = [
    "background", "aeroplane", "bicycle", "bird", "boat",
    "bottle", "bus", "car", "cat", "chair",
    "cow", "diningtable", "dog", "horse", "motorbike",
    "person", "pottedplant", "sheep", "sofa", "train", "tvmonitor",
]
_CLASS_CAT = _LABELS.index("cat")
_CLASS_DOG = _LABELS.index("dog")
_INPUT_SIZE = 300

# 模型缓存：key=(proto_path, model_path)，只加载一次
_NET_CACHE: dict[tuple[str, str], cv2.dnn.Net] = {}
_NET_LOCK = threading.Lock()


def scan_subjects(
    asset_path: str,
    *,
    proto_path: str,
    model_path: str,
    threshold: float = 0.4,
    multi_scale: bool = True,
    scales: list[float] | None = None,
    tiling: bool = False,
    tile_grid: tuple[int, int] = (2, 2),
    nms_iou: float = 0.4,
) -> list[ScanHit]:
    """
    使用 MobileNet SSD 识别画面里的关注对象，返回识别结果列表。

    处理步骤：
    1. letterbox 缩放到 300×300
    2. blobFromImage 预处理
    3. cv2.dnn 前向推理
    4. 可选多尺度 + 可选分块
    5. NMS 去重
    """
    image = cv2.imread(asset_path)
    if image is None:
        raise ValueError(f"画面读取失败: {asset_path}")

    net = _load_net(proto_path, model_path)
    h, w = image.shape[:2]

    effective_scales = scales if (multi_scale and scales) else ([1.0, 1.2, 1.5] if multi_scale else [1.0])

    raw: list[dict] = []

    if tiling:
        rows, cols = tile_grid
        for x1, y1, x2, y2 in _generate_tiles(w, h, rows, cols):
            tile = image[y1:y2, x1:x2]
            if tile.size == 0:
                continue
            for det in _scan_scaled(tile, net, threshold, effective_scales):
                raw.append({
                    "label": det["label"],
                    "confidence": det["confidence"],
                    "box": (
                        _clamp(det["box"][0] + x1, 0, w - 1),
                        _clamp(det["box"][1] + y1, 0, h - 1),
                        _clamp(det["box"][2] + x1, 0, w - 1),
                        _clamp(det["box"][3] + y1, 0, h - 1),
                    ),
                })
    else:
        raw = _scan_scaled(image, net, threshold, effective_scales)

    final = _nms(raw, iou_threshold=nms_iou)

    return [
        ScanHit(
            label=det["label"],
            confidence=round(det["confidence"], 3),
            x=int(round(det["box"][0])),
            y=int(round(det["box"][1])),
            w=int(round(det["box"][2] - det["box"][0])),
            h=int(round(det["box"][3] - det["box"][1])),
        )
        for det in final
    ]


def _load_net(proto_path: str, model_path: str) -> cv2.dnn.Net:
    """加载 Caffe 模型，同 key 只加载一次，线程安全。"""
    key = (proto_path, model_path)
    if key in _NET_CACHE:
        return _NET_CACHE[key]
    with _NET_LOCK:
        if key not in _NET_CACHE:
            _NET_CACHE[key] = cv2.dnn.readNetFromCaffe(proto_path, model_path)
    return _NET_CACHE[key]


def _scan_scaled(image, net, threshold: float, scales: list[float]) -> list[dict]:
    ih, iw = image.shape[:2]
    results: list[dict] = []
    for scale in scales:
        if abs(scale - 1.0) < 1e-6:
            scaled = image
        else:
            scaled = cv2.resize(image, (max(1, int(round(iw * scale))), max(1, int(round(ih * scale)))))
        for det in _infer(scaled, net, threshold):
            results.append({
                "label": det["label"],
                "confidence": det["confidence"],
                "box": (
                    _clamp(det["box"][0] / scale, 0, iw - 1),
                    _clamp(det["box"][1] / scale, 0, ih - 1),
                    _clamp(det["box"][2] / scale, 0, iw - 1),
                    _clamp(det["box"][3] / scale, 0, ih - 1),
                ),
            })
    return results


def _infer(image, net, threshold: float) -> list[dict]:
    h, w = image.shape[:2]
    padded, scale, pad_x, pad_y = _letterbox(image)
    blob = cv2.dnn.blobFromImage(padded, 0.007843, (_INPUT_SIZE, _INPUT_SIZE), 127.5)
    net.setInput(blob)
    detections = net.forward()

    results = []
    for i in range(detections.shape[2]):
        conf = float(detections[0, 0, i, 2])
        if conf < threshold:
            continue
        class_id = int(detections[0, 0, i, 1])
        if class_id not in (_CLASS_CAT, _CLASS_DOG):
            continue

        box = detections[0, 0, i, 3:7] * _INPUT_SIZE
        sx, sy, ex, ey = box.astype("float")
        sx = _clamp((sx - pad_x) / scale, 0, w - 1)
        sy = _clamp((sy - pad_y) / scale, 0, h - 1)
        ex = _clamp((ex - pad_x) / scale, 0, w - 1)
        ey = _clamp((ey - pad_y) / scale, 0, h - 1)

        if ex <= sx or ey <= sy:
            continue

        results.append({"label": _LABELS[class_id], "confidence": conf, "box": (sx, sy, ex, ey)})
    return results


def _letterbox(image, size: int = _INPUT_SIZE, color: tuple = (127, 127, 127)):
    h, w = image.shape[:2]
    scale = min(size / w, size / h)
    new_w, new_h = max(1, int(round(w * scale))), max(1, int(round(h * scale)))
    resized = cv2.resize(image, (new_w, new_h))
    pad_w, pad_h = size - new_w, size - new_h
    pad_left, pad_top = pad_w // 2, pad_h // 2
    padded = cv2.copyMakeBorder(
        resized,
        pad_top, pad_h - pad_top,
        pad_left, pad_w - pad_left,
        cv2.BORDER_CONSTANT, value=color,
    )
    return padded, scale, pad_left, pad_top


def _nms(results: list[dict], iou_threshold: float = 0.4) -> list[dict]:
    if not results:
        return []

    grouped: dict[str, list[dict]] = {}
    for r in results:
        grouped.setdefault(r["label"], []).append(r)

    final: list[dict] = []
    for items in grouped.values():
        boxes = [[float(r["box"][0]), float(r["box"][1]),
                  float(r["box"][2] - r["box"][0]), float(r["box"][3] - r["box"][1])] for r in items]
        scores = [float(r["confidence"]) for r in items]
        indices = cv2.dnn.NMSBoxes(boxes, scores, score_threshold=0.0, nms_threshold=iou_threshold)
        if len(indices) == 0:
            continue
        if hasattr(indices, "flatten"):
            indices = indices.flatten().tolist()
        else:
            indices = [idx[0] if isinstance(idx, (list, tuple)) else idx for idx in indices]
        for idx in indices:
            final.append(items[int(idx)])
    return final


def _generate_tiles(w: int, h: int, rows: int, cols: int) -> list[tuple[int, int, int, int]]:
    tiles = []
    for r in range(rows):
        y1, y2 = int(round(r * h / rows)), int(round((r + 1) * h / rows))
        for c in range(cols):
            x1, x2 = int(round(c * w / cols)), int(round((c + 1) * w / cols))
            if x2 > x1 and y2 > y1:
                tiles.append((x1, y1, x2, y2))
    return tiles


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
