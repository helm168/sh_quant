"""Signal Engine 核心: Signal 类型 + 边沿触发对账 + JSON 落盘.

PRD: docs/event-warehouse-sh_quant-spec.md 同款"sh_quant 产, App 读"通路,
产物按日 JSON, App 端零计算 (见 PRD §3 数据流).

Schema (PRD §4):
{
  "market": "CN",
  "asOfDate": "2026-05-21",
  "generatedAt": "2026-05-21T18:20:00+08:00",
  "universeSize": 5187,
  "signals": [
    {
      "id": "CN-2026-05-18-STK_VOL_PERSIST-603986.SH",
      "type": "STK_VOL_PERSIST", "scope": "stock", "level": "watch",
      "severity": 72,
      "subject": {"kind": "stock", "id": "603986.SH",
                  "ticker": "603986", "name": "兆易创新"},
      "title": "...", "detail": "...", "metrics": {...},
      "firstTriggeredDate": "2026-05-18", "isNew": true
    }
  ]
}

边沿触发 (PRD §6.1): 同一 (market,type,subject.id) 在昨日 latest.json 已存在
→ 沿用昨日 firstTriggeredDate, isNew=False. 否则 firstTriggeredDate=今日,
isNew=True. id 含 firstTriggeredDate, 持续期间 id 不变.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

Scope = Literal['market', 'sector', 'stock']
Level = Literal['risk', 'watch', 'opportunity']


@dataclass
class Subject:
    kind: str                       # 'market' | 'sector' | 'stock'
    id: str                         # 大盘: market code (e.g. 'CN'); 板块: sector name; 个股: ts_code
    ticker: str | None = None       # 个股: 去后缀 code (e.g. '603986')
    name: str | None = None         # 显示名 (中/英文)


@dataclass
class Signal:
    type: str                       # 信号代号 (PRD §5), e.g. 'STK_VOL_PERSIST'
    scope: Scope
    level: Level
    severity: int                   # 0-100, 同 level 内排序
    subject: Subject
    title: str
    detail: str
    metrics: dict
    # 引擎填写:
    id: str = ''
    firstTriggeredDate: str = ''
    isNew: bool = True

    def reconcile_key(self) -> tuple[str, str]:
        """跨日对账主键 (market 由外层拼)."""
        return (self.type, self.subject.id)

    def to_json(self) -> dict:
        d = asdict(self)
        # subject 嵌套也是 asdict, 自动展开; None 字段过滤掉, 保持 JSON 干净
        d['subject'] = {k: v for k, v in d['subject'].items() if v is not None}
        return d


def _data_root() -> Path:
    """与 pull_macro / pull_sector_turnover 完全一致."""
    override = os.environ.get('SH_QUANT_DATA_DIR')
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / '.market_data'


def _sanitize_id_component(s: str) -> str:
    """subject.id 用于 signal.id, 去掉空格/斜杠等不雅字符."""
    return re.sub(r'[^A-Za-z0-9._-]+', '_', s)


def make_signal_id(market: str, signal_type: str, first_date: str, subject_id: str) -> str:
    return f'{market}-{first_date}-{signal_type}-{_sanitize_id_component(subject_id)}'


def load_previous(market: str, out_dir: Path) -> dict[tuple[str, str], dict]:
    """读 <market>_latest.json, 返回 {(type,subject_id): prev_signal_dict}.

    缺文件 (首次跑) 返回空 dict, 所有信号都会 isNew=True.
    """
    fp = out_dir / f'{market.lower()}_latest.json'
    if not fp.exists():
        return {}
    try:
        prev = json.loads(fp.read_text())
    except (json.JSONDecodeError, OSError) as e:
        # 上次写坏了不算事故, 当作冷启动; 但要打到 stdout 让 cron 日志可见
        print(f'  [WARN] previous {fp.name} unreadable ({e}); treating all signals as new')
        return {}
    out = {}
    for s in prev.get('signals', []):
        out[(s['type'], s['subject']['id'])] = s
    return out


def reconcile(
    signals: list[Signal],
    market: str,
    as_of_date: str,
    previous: dict[tuple[str, str], dict],
) -> list[Signal]:
    """边沿触发: 比对昨日 latest, 填 firstTriggeredDate / isNew / id."""
    for s in signals:
        key = s.reconcile_key()
        prev = previous.get(key)
        if prev:
            s.firstTriggeredDate = prev.get('firstTriggeredDate', as_of_date)
            s.isNew = False
        else:
            s.firstTriggeredDate = as_of_date
            s.isNew = True
        s.id = make_signal_id(market, s.type, s.firstTriggeredDate, s.subject.id)
    return signals


def _level_order(level: Level) -> int:
    return {'risk': 0, 'watch': 1, 'opportunity': 2}[level]


def sort_signals(signals: list[Signal]) -> list[Signal]:
    """风险 → 关注 → 机会; 同 level 内 severity 降序."""
    return sorted(signals, key=lambda s: (_level_order(s.level), -s.severity))


def write_output(
    market: str,
    as_of_date: str,
    universe_size: int,
    signals: list[Signal],
    out_dir: Path,
) -> tuple[Path, Path]:
    """落盘 <market>_<date>.json + <market>_latest.json. 返回 (dated_fp, latest_fp)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        'market': market,
        'asOfDate': as_of_date,
        'generatedAt': datetime.now().astimezone().isoformat(timespec='seconds'),
        'universeSize': universe_size,
        'signals': [s.to_json() for s in sort_signals(signals)],
    }
    m = market.lower()
    dated_fp = out_dir / f'{m}_{as_of_date}.json'
    latest_fp = out_dir / f'{m}_latest.json'
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    dated_fp.write_text(body)
    latest_fp.write_text(body)
    return dated_fp, latest_fp
