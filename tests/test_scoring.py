"""utils/scoring.py 关键不变量测试。

核心 case 用真实 688525 佰维存储 年报数字驱动：净利序列(中间巨亏 + 周期顶点)
必须把分数压垮，营收序列(逐年真增长)必须基本不动。这是本模块存在的理由，
破了立刻 fail。
"""

from __future__ import annotations

import math

import pytest

from utils.scoring import growth_score

# 688525 佰维存储 年报口径(FY2022→FY2025)，单位元，取自 data_cache/financials。
BWC_REVENUE = [2.985693e9, 3.590752e9, 6.695185e9, 1.130248e10]
BWC_NET_INCOME = [7.121873e7, -6.308675e8, 1.352442e8, 8.388462e8]


def test_raw_cagr_is_endpoint_formula():
    """raw_cagr 必须就是首末两点的 CAGR，不被路径惩罚改写。"""
    r = growth_score(BWC_REVENUE)
    expected = (BWC_REVENUE[-1] / BWC_REVENUE[0]) ** (1 / 3) - 1
    assert r.raw_cagr == pytest.approx(expected)
    assert r.raw_cagr == pytest.approx(0.5585, abs=1e-3)  # app 上显示的 55.85%


def test_clean_compounder_no_penalty():
    """每年都涨、基数不畸小、无亏损年 → quality=1，score=headline。"""
    r = growth_score([100, 130, 169, 219.7])  # 精确 30%/年
    assert r.raw_cagr == pytest.approx(0.30, abs=1e-3)
    assert r.quality == pytest.approx(1.0)
    assert r.cagr_reliable is True
    assert r.flags == ()
    assert r.score == pytest.approx(100.0)  # 30% 命中默认饱和点


def test_bwc_revenue_leg_survives():
    """营收逐年真增长 → 这条腿应当几乎不被惩罚（保留可信增长）。"""
    r = growth_score(BWC_REVENUE)
    assert r.cagr_reliable is True
    assert r.flags == ()
    assert r.quality == pytest.approx(1.0)
    assert r.score == pytest.approx(100.0)


def test_bwc_net_income_leg_collapses():
    """净利两点 CAGR=127.5% 但中途巨亏 + 基数畸小 → 分数必须被压垮。"""
    r = growth_score(BWC_NET_INCOME)
    assert r.raw_cagr == pytest.approx(1.2753, abs=1e-3)  # 两点 CAGR 仍然算得出
    assert r.headline_score == pytest.approx(100.0)  # 未惩罚时会误判 100
    assert r.cagr_reliable is False
    assert set(r.flags) == {'loss_year', 'tiny_base', 'non_monotonic'}
    assert r.score < 10  # 惩罚后塌到个位数


def test_negative_base_turnaround_scores_zero():
    """基期为负(亏损中)→ CAGR 无定义，headline=0，分数=0 且标记不可靠。"""
    r = growth_score([-50, 20, 80, 200])
    assert r.raw_cagr is None
    assert r.cagr_reliable is False
    assert 'negative_base' in r.flags
    assert r.score == pytest.approx(0.0)


def test_end_negative_flags_loss_and_no_cagr():
    """末期转亏 → CAGR 无定义，必带 loss_year，分数为 0。"""
    r = growth_score([100, 120, 90, -30])
    assert r.raw_cagr is None
    assert 'loss_year' in r.flags
    assert r.cagr_reliable is False
    assert r.score == pytest.approx(0.0)


def test_monotonic_penalty_scales_with_dip_depth():
    """中途回撤越深，扣分越多；浅回撤的分应高于深回撤。"""
    shallow = growth_score([100, 140, 130, 200])  # 小回撤
    deep = growth_score([100, 200, 105, 210])  # 大回撤
    assert 'non_monotonic' in shallow.flags
    assert 'non_monotonic' in deep.flags
    assert shallow.score > deep.score


def test_saturate_at_is_tunable():
    """放大饱和点 → 同一 CAGR 的 headline 线性变小。"""
    fast = growth_score([100, 130, 169, 219.7], saturate_at=0.30)
    slow = growth_score([100, 130, 169, 219.7], saturate_at=0.60)
    assert fast.headline_score == pytest.approx(100.0)
    assert slow.headline_score == pytest.approx(50.0)


def test_min_base_frac_is_tunable():
    """放宽 min_base_frac 后，原本的 tiny_base 警告应消失。"""
    strict = growth_score(BWC_NET_INCOME)  # 默认 0.15 → tiny_base
    loose = growth_score(BWC_NET_INCOME, min_base_frac=0.0)
    assert 'tiny_base' in strict.flags
    assert 'tiny_base' not in loose.flags


def test_too_few_points_raises():
    with pytest.raises(ValueError, match='至少要 2'):
        growth_score([42.0])


def test_non_finite_raises():
    with pytest.raises(ValueError, match='NaN/Inf'):
        growth_score([100.0, math.nan, 200.0])


def test_two_dim_input_raises():
    with pytest.raises(ValueError, match='一维'):
        growth_score([[1, 2], [3, 4]])
