"""notify_digest._passes_user_filter — 用户视角过滤逻辑."""

from __future__ import annotations

# 直接 import 单个函数, scripts/ 作为 module 需要把 worktree root 加进 sys.path
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.notify_digest import _passes_user_filter  # noqa: E402


def _sig(scope: str, level: str, sid: str = '600519.SH') -> dict:
    return {'scope': scope, 'level': level, 'subject': {'kind': scope, 'id': sid}}


# ── 用户配置: 风险只看持仓, 机会只看 AI 链, 大盘 risk 不推 ──
USER_FILTER_DEFAULT = {
    'portfolio': {'600519.SH', '603986.SH'},
    'opp_universe': {'603986.SH', '688981.SH'},  # 兆易/中芯
    'push_market_risk': False,
    'risk_portfolio_only': True,
}


def test_market_risk_blocked():
    """大盘 MKT_STREAK risk 在 push_market_risk=False 时不推."""
    assert _passes_user_filter(_sig('market', 'risk'), USER_FILTER_DEFAULT) is False


def test_market_watch_passes():
    """大盘 watch (如 MKT_TURNOVER_COLD) 不受 push_market_risk 影响, 默认推."""
    assert _passes_user_filter(_sig('market', 'watch'), USER_FILTER_DEFAULT) is True


def test_market_opportunity_passes():
    assert _passes_user_filter(_sig('market', 'opportunity'), USER_FILTER_DEFAULT) is True


def test_sector_risk_blocked():
    assert _passes_user_filter(_sig('sector', 'risk'), USER_FILTER_DEFAULT) is False


def test_stock_risk_in_portfolio():
    """持仓股的 risk 信号必推."""
    s = _sig('stock', 'risk', '600519.SH')
    assert _passes_user_filter(s, USER_FILTER_DEFAULT) is True


def test_stock_risk_not_in_portfolio_blocked():
    """非持仓的 risk 信号不推 (risk_portfolio_only=True)."""
    s = _sig('stock', 'risk', '000001.SZ')
    assert _passes_user_filter(s, USER_FILTER_DEFAULT) is False


def test_stock_opportunity_in_universe():
    """机会信号: 落在 AI 链内才推."""
    s = _sig('stock', 'opportunity', '688981.SH')
    assert _passes_user_filter(s, USER_FILTER_DEFAULT) is True


def test_stock_opportunity_outside_universe_blocked():
    """非 AI 链的 STK_BREAKOUT 不推 (主要噪音来源)."""
    s = _sig('stock', 'opportunity', '600519.SH')  # 茅台在 portfolio 但不在 opp
    assert _passes_user_filter(s, USER_FILTER_DEFAULT) is False


def test_stock_watch_portfolio_passes():
    """watch 是中性 — portfolio 内的票推."""
    s = _sig('stock', 'watch', '600519.SH')
    assert _passes_user_filter(s, USER_FILTER_DEFAULT) is True


def test_stock_watch_opp_universe_passes():
    """watch 是中性 — AI 链的票也推."""
    s = _sig('stock', 'watch', '688981.SH')
    assert _passes_user_filter(s, USER_FILTER_DEFAULT) is True


def test_stock_watch_neither_blocked():
    """watch 既不在 portfolio 也不在 AI 链 → 不推 (避免噪音)."""
    s = _sig('stock', 'watch', '000001.SZ')
    assert _passes_user_filter(s, USER_FILTER_DEFAULT) is False


def test_empty_portfolio_kills_risk_channel():
    """portfolio 空时 (用户还没填持仓), 股票 risk 一条不推 — 设计预期."""
    uf = {**USER_FILTER_DEFAULT, 'portfolio': set()}
    s = _sig('stock', 'risk', '600519.SH')
    assert _passes_user_filter(s, uf) is False


def test_no_opp_universe_means_no_filter():
    """opp_universe=None (没配主题) 时不过滤机会 — 兼容老用户."""
    uf = {**USER_FILTER_DEFAULT, 'opp_universe': None}
    s = _sig('stock', 'opportunity', '000001.SZ')
    assert _passes_user_filter(s, uf) is True


def test_risk_portfolio_only_off_lets_all_risk_through():
    """关掉 risk_portfolio_only → 全市场 risk 都推 (噪音模式)."""
    uf = {**USER_FILTER_DEFAULT, 'risk_portfolio_only': False}
    s = _sig('stock', 'risk', '000001.SZ')
    assert _passes_user_filter(s, uf) is True
