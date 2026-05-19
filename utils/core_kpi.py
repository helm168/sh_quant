"""config/core_kpi.yaml 的查询接口。

加载一次，缓存在内存。按 A 方案这份 yaml 是核心关切点的 single source of
truth（thesis / expected / polarity 都在这），所以 _load 时做一层轻量校验，
yaml 写错（枚举打错、缺必填）当场报错，而不是把坏数据喂给下游徽章逻辑。

给 notebook / pull_kpi / 其他模块提供：
    list_kpis()                - 列出所有 kpi_id
    get_kpi(kpi_id)            - 取完整 KPI 字典
    get_kpis()                 - 取全部 {kpi_id: 定义}
    list_kpis_for(ticker)      - 反查：某只票关联了哪些 kpi_id
    kpis_by_category(cat)      - 按 category 过滤
    auto_fetchable()           - source.kind != manual 的 kpi_id（pull_kpi 用）
    reload_kpis()              - 清缓存重读（编辑过 yaml 后用）

用法（notebook 里）：
    from utils.core_kpi import get_kpi, list_kpis_for

    for kid in list_kpis_for('600519.SH'):
        print(kid, get_kpi(kid)['thesis'])
"""

from __future__ import annotations

from functools import cache

import yaml

from config import ROOT_DIR

CORE_KPI_YAML = ROOT_DIR / 'config' / 'core_kpi.yaml'

_CATEGORIES = {'industry', 'company', 'supply_chain', 'policy'}
_CADENCES = {'daily', 'weekly', 'monthly', 'quarterly'}
_POLARITIES = {'higher_is_better', 'lower_is_better', 'in_range'}
_SOURCE_KINDS = {'api', 'url', 'manual'}
_REQUIRED = ('name', 'tickers', 'category', 'cadence', 'polarity', 'source', 'thesis')


def _validate(kpis: dict) -> None:
    """yaml 单一真相，写错当场炸。逐 kpi 检必填 + 枚举合法。"""
    for kid, d in kpis.items():
        if not isinstance(d, dict):
            raise ValueError(f'KPI {kid!r} 定义不是字典')
        missing = [k for k in _REQUIRED if k not in d]
        if missing:
            raise ValueError(f'KPI {kid!r} 缺必填字段: {missing}')
        if not isinstance(d['tickers'], list) or not d['tickers']:
            raise ValueError(f'KPI {kid!r} 的 tickers 必须是非空列表')
        if d['category'] not in _CATEGORIES:
            raise ValueError(f'KPI {kid!r} category={d["category"]!r} 非法，可用: {sorted(_CATEGORIES)}')
        if d['cadence'] not in _CADENCES:
            raise ValueError(f'KPI {kid!r} cadence={d["cadence"]!r} 非法，可用: {sorted(_CADENCES)}')
        if d['polarity'] not in _POLARITIES:
            raise ValueError(f'KPI {kid!r} polarity={d["polarity"]!r} 非法，可用: {sorted(_POLARITIES)}')
        src = d['source']
        if not isinstance(src, dict) or src.get('kind') not in _SOURCE_KINDS:
            raise ValueError(
                f'KPI {kid!r} source.kind 缺失或非法，可用: {sorted(_SOURCE_KINDS)}'
            )


@cache
def _load() -> dict:
    """加载 core_kpi.yaml，缓存在内存。失败时抛带提示的错误。"""
    if not CORE_KPI_YAML.exists():
        raise FileNotFoundError(f'找不到 {CORE_KPI_YAML}')
    with open(CORE_KPI_YAML, encoding='utf-8') as f:
        data = yaml.safe_load(f)
    if not data or 'kpis' not in data:
        raise ValueError('core_kpi.yaml 顶层缺少 `kpis` 字段')
    kpis = data['kpis']
    _validate(kpis)
    return kpis


def reload_kpis() -> None:
    """清缓存。编辑过 core_kpi.yaml 后调用，下次访问会重新读盘。"""
    _load.cache_clear()


def list_kpis() -> list[str]:
    """所有 kpi_id（按 yaml 中的顺序）。"""
    return list(_load().keys())


def get_kpi(kpi_id: str) -> dict:
    """返回完整 KPI 字典。"""
    kpis = _load()
    if kpi_id not in kpis:
        raise KeyError(f'未知 KPI {kpi_id!r}。已定义: {list(kpis.keys())}')
    return kpis[kpi_id]


def get_kpis() -> dict[str, dict]:
    """返回全部 {kpi_id: 定义}（浅拷贝，改它不影响缓存）。"""
    return dict(_load())


def list_kpis_for(ticker: str) -> list[str]:
    """反查：给一个 ticker（ts_code 或境外代码），返回关联到它的 kpi_id 列表。

    注意一只票可关联多个 KPI；返回顺序按 yaml 中 KPI 定义顺序。
    """
    return [kid for kid, d in _load().items() if ticker in d.get('tickers', [])]


def kpis_by_category(category: str) -> list[str]:
    """按 category（industry/company/supply_chain/policy）过滤 kpi_id。"""
    if category not in _CATEGORIES:
        raise ValueError(f'未知 category {category!r}，可用: {sorted(_CATEGORIES)}')
    return [kid for kid, d in _load().items() if d['category'] == category]


def auto_fetchable() -> list[str]:
    """source.kind != manual 的 kpi_id —— pull_kpi 能自动抓的那批。"""
    return [kid for kid, d in _load().items() if d['source']['kind'] != 'manual']
