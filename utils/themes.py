"""config/themes.yaml 的查询接口。

加载一次，缓存在内存。给 notebook / 其他 utils 模块提供：
    list_themes()              - 列出所有 theme_id
    get_theme(theme_id)        - 取完整 theme 字典
    get_subtracks(theme_id)    - 取该主题的子方向定义 {id: 显示名}
    get_stocks(theme_id, ...)  - 取股票列表（可按 subtrack 过滤）
    get_codes(theme_id, ...)   - 便捷：只返回 ts_code 列表
    find_stock(ts_code)        - 反向查找股票出现在哪些 (theme_id, subtrack) 对里
    reload_themes()            - 清缓存重读（编辑过 yaml 后用）

用法（notebook 里）：
    from utils.themes import get_codes, get_stocks, get_subtracks

    chips = get_codes('ai_compute', subtrack='chip')
    # ['688256.SH', '688041.SH', ...]

    for sub_id, sub_name in get_subtracks('ai_compute').items():
        codes = get_codes('ai_compute', subtrack=sub_id)
        print(f'{sub_id} ({sub_name}): {len(codes)} 只')
"""

from __future__ import annotations

from functools import cache

import yaml

from config import ROOT_DIR

THEMES_YAML = ROOT_DIR / 'config' / 'themes.yaml'


@cache
def _load() -> dict:
    """加载 themes.yaml，缓存在内存。失败时抛带提示的错误。"""
    if not THEMES_YAML.exists():
        raise FileNotFoundError(f'找不到 {THEMES_YAML}')
    with open(THEMES_YAML, encoding='utf-8') as f:
        data = yaml.safe_load(f)
    if not data or 'themes' not in data:
        raise ValueError('themes.yaml 顶层缺少 `themes` 字段')
    return data['themes']


def reload_themes() -> None:
    """清缓存。编辑过 themes.yaml 后调用，下次访问会重新读盘。"""
    _load.cache_clear()


def list_themes() -> list[str]:
    """所有 theme_id（按 yaml 中的顺序）。"""
    return list(_load().keys())


def get_theme(theme_id: str) -> dict:
    """返回完整 theme 字典（含 name / desc / subtracks / stocks）。"""
    themes = _load()
    if theme_id not in themes:
        raise KeyError(f'未知主题 {theme_id!r}。已定义: {list(themes.keys())}')
    return themes[theme_id]


def get_subtracks(theme_id: str) -> dict[str, str]:
    """返回该主题的 subtrack 字典 {id: 显示名}；未定义子方向则返回空 dict。"""
    return get_theme(theme_id).get('subtracks', {}) or {}


def get_stocks(theme_id: str, subtrack: str | None = None) -> list[dict]:
    """返回 [{code, name, subtrack}, ...] 列表。

    Args:
        theme_id: 主题 id（见 themes.yaml 顶层）
        subtrack: 可选。只返回属于该 subtrack 的股票。
                  该主题必须已定义此 subtrack（否则 ValueError）。
    """
    theme = get_theme(theme_id)
    stocks = theme.get('stocks', []) or []

    if subtrack is None:
        return list(stocks)

    defined = set(get_subtracks(theme_id).keys())
    if not defined:
        raise ValueError(
            f'主题 {theme_id!r} 没有定义 subtracks，无法按 subtrack={subtrack!r} 过滤。'
        )
    if subtrack not in defined:
        raise ValueError(f'主题 {theme_id!r} 没有 subtrack={subtrack!r}。可用: {sorted(defined)}')

    return [s for s in stocks if s.get('subtrack') == subtrack]


def get_codes(theme_id: str, subtrack: str | None = None) -> list[str]:
    """便捷函数：只返回 ts_code 字符串列表。"""
    return [s['code'] for s in get_stocks(theme_id, subtrack)]


def find_stock(ts_code: str) -> list[tuple[str, str | None]]:
    """反向查找：给一个 ts_code，返回它出现在哪些 (theme_id, subtrack) 对里。

    注意同一只股票可能在多个主题里。返回顺序按 yaml 中主题定义顺序。
    """
    out: list[tuple[str, str | None]] = []
    for theme_id, theme in _load().items():
        for s in theme.get('stocks', []) or []:
            if s.get('code') == ts_code:
                out.append((theme_id, s.get('subtrack')))
    return out
