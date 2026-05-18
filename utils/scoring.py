"""成长打分：路径质量惩罚版。

为什么不用两点 CAGR：见 688525 佰维存储 的案例 —— FY2022(周期低点 0.71 亿)
→ FY2025(周期高点 8.39 亿) 的两点净利 CAGR 高达 127.53%，但中间 FY2023 巨亏
-6.31 亿、FY2025 利润几乎全砸在 Q4。两点 CAGR 把"差点暴雷 + 周期顶点"完全抹平，
对强周期标的会给出误导性的"高增长"。

本模块在 CAGR 基础上叠加**路径质量惩罚**：

  - 窗口内出现亏损年          → 强扣分 + 标记 CAGR 不可靠
  - 基期 ≤ 0 或畸小(相对峰值)  → CAGR 失真，扣分 + 标记不可靠
  - 逐年非单调(中途回撤)      → 按回撤幅度比例扣分

干净的复利序列(每年都涨、基数不畸小、无亏损年)惩罚为 0，分数 = headline；
路径越脏，quality 越低，最终分被压向 0。

只处理**年度**序列；季度 / TTM 口径请先聚合成年度再传进来。值按时间
**从旧到新**排列。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class GrowthScore:
    """成长打分结果。

    raw_cagr       端点 CAGR；基期或末期 ≤ 0 时为 None(此时无法定义增长率)。
    headline_score CAGR 映射出的原始分(0-100)，未经路径惩罚。
    quality        路径质量乘子(0-1)，1=干净复利，越低路径越脏。
    score          最终分 = headline_score * quality，裁剪到 [0, 100]。
    cagr_reliable  CAGR 是否可信(无亏损年、基数不畸小、基末期均 > 0)。
    flags          触发的惩罚项：loss_year / negative_base / tiny_base / non_monotonic。
    """

    raw_cagr: float | None
    headline_score: float
    quality: float
    score: float
    cagr_reliable: bool
    flags: tuple[str, ...]


def growth_score(
    values,
    *,
    saturate_at: float = 0.30,
    min_base_frac: float = 0.15,
    loss_penalty: float = 0.45,
    base_penalty: float = 0.30,
    mono_penalty: float = 0.40,
) -> GrowthScore:
    """对一段年度财务序列做路径质量惩罚后的成长打分。

    参数
    ----
    values         年度序列(营收 / 净利 ...)，从旧到新，长度 ≥ 2。
    saturate_at    CAGR 映射饱和点：CAGR ≥ 该值 → headline 满分 100。默认 30%。
    min_base_frac  基期低于 `窗口峰值 * 该比例` 视为"基数畸小"(CAGR 失真)。默认 0.15。
    loss_penalty   出现亏损年的扣分(从 quality=1 里减)。默认 0.45。
    base_penalty   基期 ≤ 0 或畸小的扣分。默认 0.30。
    mono_penalty   非单调的最大扣分，实际按回撤幅度占全程比例线性缩放。默认 0.40。
    """
    arr = np.asarray(values, dtype=float)
    if arr.ndim != 1 or arr.size < 2:
        raise ValueError('values 至少要 2 个年度点，且为一维序列')
    if not np.isfinite(arr).all():
        raise ValueError('values 含 NaN/Inf；请先清洗财务序列')

    base, end = float(arr[0]), float(arr[-1])
    years = arr.size - 1
    peak = float(arr.max())

    flags: list[str] = []
    reliable = True

    # 1) 端点 CAGR + headline 映射 ----------------------------------------
    if base > 0 and end > 0:
        raw_cagr: float | None = (end / base) ** (1 / years) - 1
        headline = float(np.clip(raw_cagr / saturate_at, 0.0, 1.0) * 100)
    else:
        # 基期或末期非正 → 增长率无定义，headline 给 0，由下面的惩罚兜底
        raw_cagr = None
        headline = 0.0

    # 2) 路径质量惩罚 ------------------------------------------------------
    deductions = 0.0

    # 2a 亏损年：窗口内任意一年 ≤ 0
    if (arr <= 0).any():
        deductions += loss_penalty
        flags.append('loss_year')
        reliable = False

    # 2b 基数问题：≤ 0 直接失真；> 0 但相对峰值畸小同样让 CAGR 虚高
    if base <= 0:
        deductions += base_penalty
        flags.append('negative_base')
        reliable = False
    elif peak > 0 and base < min_base_frac * peak:
        deductions += base_penalty
        flags.append('tiny_base')
        reliable = False

    # 2c 非单调：按"逐年下跌总幅度 / 全程极差"比例扣分
    drops = np.maximum(arr[:-1] - arr[1:], 0.0)
    total_down = float(drops.sum())
    span = peak - float(arr.min())
    if span > 0 and total_down > 0:
        violation = min(total_down / span, 1.0)
        deductions += mono_penalty * violation
        flags.append('non_monotonic')

    quality = float(np.clip(1.0 - deductions, 0.0, 1.0))
    score = float(np.clip(headline * quality, 0.0, 100.0))

    return GrowthScore(
        raw_cagr=raw_cagr,
        headline_score=headline,
        quality=quality,
        score=score,
        cagr_reliable=reliable,
        flags=tuple(flags),
    )
