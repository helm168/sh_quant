"""utils/themes.py 关键不变量测试。

注：测试直接读真实 config/themes.yaml。如果以后大改 yaml 内容（比如删掉
ai_compute 主题），这里几个 case 需要同步更新——这是有意为之，让 yaml
的"契约"破坏时立刻有 fail 提醒。
"""

from __future__ import annotations

import pytest

from utils import themes


def test_list_themes_includes_known_ones():
    ids = themes.list_themes()
    assert 'ai_compute' in ids
    assert 'baijiu' in ids
    assert len(ids) >= 17   # 当前 22 个，留余量防误删


def test_get_theme_returns_full_dict():
    theme = themes.get_theme('ai_compute')
    assert 'name' in theme
    assert 'stocks' in theme
    assert 'subtracks' in theme   # ai_compute 应该有 subtrack 定义


def test_get_theme_unknown_raises():
    with pytest.raises(KeyError, match='未知主题'):
        themes.get_theme('not_a_real_theme_xyz')


def test_get_subtracks_for_themed_subtrack():
    sub = themes.get_subtracks('ai_compute')
    assert 'chip' in sub
    assert 'memory' in sub


def test_get_subtracks_for_flat_theme_returns_empty():
    """没定义 subtracks 的主题应该返回空 dict（不抛错）。"""
    assert themes.get_subtracks('baijiu') == {}


def test_get_stocks_no_filter_returns_all():
    stocks = themes.get_stocks('ai_compute')
    assert len(stocks) > 30
    sample = stocks[0]
    assert 'code' in sample
    assert 'name' in sample


def test_get_stocks_filter_by_subtrack():
    chips = themes.get_stocks('ai_compute', subtrack='chip')
    assert all(s.get('subtrack') == 'chip' for s in chips)
    codes = [s['code'] for s in chips]
    assert '688256.SH' in codes   # 寒武纪是 chip subtrack 的代表


def test_get_stocks_unknown_subtrack_raises():
    with pytest.raises(ValueError, match='subtrack'):
        themes.get_stocks('ai_compute', subtrack='not_a_subtrack')


def test_get_stocks_subtrack_on_flat_theme_raises():
    """对没定义 subtracks 的主题传 subtrack 参数应该抛错。"""
    with pytest.raises(ValueError, match='没有定义 subtracks'):
        themes.get_stocks('baijiu', subtrack='whatever')


def test_get_codes_returns_strings_with_suffix():
    codes = themes.get_codes('ai_compute', subtrack='chip')
    assert len(codes) > 0
    assert all(isinstance(c, str) for c in codes)
    assert all(c.endswith(('.SH', '.SZ', '.BJ')) for c in codes)


def test_find_stock_finds_overlaps():
    """002460.SZ 赣锋锂业 应该至少出现在 ev_battery / advanced_battery / nonferrous_metals 中。"""
    hits = themes.find_stock('002460.SZ')
    theme_ids = {h[0] for h in hits}
    assert 'ev_battery' in theme_ids
    assert len(theme_ids) >= 2   # 至少 2 个主题里


def test_find_stock_unknown_returns_empty():
    assert themes.find_stock('FAKE99.SZ') == []
