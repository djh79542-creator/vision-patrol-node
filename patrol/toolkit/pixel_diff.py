from __future__ import annotations

import os
import cv2
import numpy as np
from dataclasses import dataclass


@dataclass
class DiffReport:
    ok: bool
    ratio: float           # 变化面积占比（0-100）
    conclusion: str        # "change" | "no_change" | "error"
    error: str | None = None


def diff_frames(
    image_path1: str,
    image_path2: str,
    *,
    thresh: int = 25,
    auto_thresh: bool = True,
    blur: int = 21,
    dilate_iters: int = 2,
    min_area: float = 500.0,
    ratio_threshold: float = 1.0,
    illumination: str = "clahe",
    edge: str = "sobel",
    clahe_clip: float = 2.0,
    clahe_grid: int = 8,
    retinex_sigma: float = 15.0,
    sobel_ksize: int = 3,
    canny_low: int = 50,
    canny_high: int = 150,
) -> DiffReport:
    """
    比较两张图片，返回变化面积占比与结论。

    处理步骤：
    1. 转灰度
    2. 光照归一化（clahe / retinex / none）
    3. 高斯模糊
    4. 边缘增强（sobel / canny / none）
    5. absdiff → 二值化（Otsu 或固定阈值）
    6. 膨胀 → 找轮廓 → 累计超 min_area 的面积
    7. 计算占比，按 ratio_threshold 给出结论
    """
    if not os.path.exists(image_path1) or not os.path.exists(image_path2):
        return DiffReport(ok=False, ratio=0.0, conclusion="error", error="图片文件不存在")

    img1 = cv2.imread(image_path1)
    img2 = cv2.imread(image_path2)
    if img1 is None or img2 is None:
        return DiffReport(ok=False, ratio=0.0, conclusion="error", error="画面读取失败")

    img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))

    gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)

    gray1 = _equalise_light(gray1, illumination, clahe_clip, clahe_grid, retinex_sigma)
    gray2 = _equalise_light(gray2, illumination, clahe_clip, clahe_grid, retinex_sigma)

    ksize = blur if blur % 2 == 1 else blur + 1
    ksize = max(3, ksize)
    gray1 = cv2.GaussianBlur(gray1, (ksize, ksize), 0)
    gray2 = cv2.GaussianBlur(gray2, (ksize, ksize), 0)

    gray1 = _boost_edges(gray1, edge, sobel_ksize, canny_low, canny_high)
    gray2 = _boost_edges(gray2, edge, sobel_ksize, canny_low, canny_high)

    delta = cv2.absdiff(gray1, gray2)

    if auto_thresh:
        _, thresh_img = cv2.threshold(delta, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
    else:
        _, thresh_img = cv2.threshold(delta, thresh, 255, cv2.THRESH_BINARY)

    if dilate_iters > 0:
        thresh_img = cv2.dilate(thresh_img, None, iterations=dilate_iters)

    contours_data = cv2.findContours(thresh_img.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    contours = contours_data[0] if len(contours_data) == 2 else contours_data[1]

    total_area = float(img1.shape[0] * img1.shape[1])
    change_area = 0.0
    for c in contours:
        area = cv2.contourArea(c)
        if area >= min_area:
            change_area += area

    ratio = (change_area / total_area * 100) if total_area > 0 else 0.0
    conclusion = "change" if ratio > ratio_threshold else "no_change"

    return DiffReport(ok=True, ratio=ratio, conclusion=conclusion)


def _equalise_light(
    gray,
    method: str,
    clahe_clip: float,
    clahe_grid: int,
    retinex_sigma: float,
):
    if method == "clahe":
        grid = (max(2, clahe_grid), max(2, clahe_grid))
        clahe = cv2.createCLAHE(clipLimit=clahe_clip, tileGridSize=grid)
        return clahe.apply(gray)
    if method == "retinex":
        gray_f = gray.astype("float32")
        blur = cv2.GaussianBlur(gray_f, (0, 0), max(1.0, retinex_sigma))
        ret = cv2.log(gray_f + 1.0) - cv2.log(blur + 1.0)
        ret = cv2.normalize(ret, None, 0, 255, cv2.NORM_MINMAX)
        return ret.astype("uint8")
    return gray


def _boost_edges(gray, method: str, sobel_ksize: int, canny_low: int, canny_high: int):
    if method == "sobel":
        ksize = sobel_ksize if sobel_ksize % 2 == 1 else sobel_ksize + 1
        ksize = max(3, min(31, ksize))
        gx = cv2.Sobel(gray, cv2.CV_32F, 1, 0, ksize=ksize)
        gy = cv2.Sobel(gray, cv2.CV_32F, 0, 1, ksize=ksize)
        mag = cv2.magnitude(gx, gy)
        mag = cv2.normalize(mag, None, 0, 255, cv2.NORM_MINMAX)
        return mag.astype("uint8")
    if method == "canny":
        low = max(0, canny_low)
        high = max(low + 1, canny_high)
        return cv2.Canny(gray, low, high)
    return gray
