"""Signal Engine 边沿触发 / id 稳定性单测 (PRD §8 验收 2)."""

from __future__ import annotations

import json

from utils.signals_engine import (
    Signal,
    Subject,
    load_previous,
    make_signal_id,
    reconcile,
    sort_signals,
    write_output,
)


def _mk(stype: str, sid: str, level: str = 'watch', sev: int = 50) -> Signal:
    return Signal(
        type=stype, scope='stock', level=level, severity=sev,
        subject=Subject(kind='stock', id=sid, ticker=sid.split('.')[0]),
        title='t', detail='d', metrics={},
    )


def test_first_run_all_new(tmp_path):
    sigs = [_mk('STK_VOL_PERSIST', '600519.SH')]
    out = reconcile(sigs, 'CN', '2026-05-22', previous={})
    assert out[0].isNew is True
    assert out[0].firstTriggeredDate == '2026-05-22'
    assert out[0].id == 'CN-2026-05-22-STK_VOL_PERSIST-600519.SH'


def test_second_run_carries_forward(tmp_path):
    # 跑 D-1 写入
    sigs = [_mk('STK_VOL_PERSIST', '600519.SH')]
    reconcile(sigs, 'CN', '2026-05-22', previous={})
    write_output('CN', '2026-05-22', universe_size=10, signals=sigs, out_dir=tmp_path)

    # D 再算同一只票同一信号
    new_sigs = [_mk('STK_VOL_PERSIST', '600519.SH')]
    prev = load_previous('CN', tmp_path)
    reconciled = reconcile(new_sigs, 'CN', '2026-05-23', previous=prev)
    assert reconciled[0].isNew is False
    assert reconciled[0].firstTriggeredDate == '2026-05-22'  # 沿用首次日
    assert reconciled[0].id.endswith('STK_VOL_PERSIST-600519.SH')
    assert '2026-05-22' in reconciled[0].id  # id 含首次日, 持续期不变


def test_signal_lapses_then_re_triggers(tmp_path):
    # D-1: 触发
    write_output('CN', '2026-05-20', universe_size=10,
                 signals=reconcile([_mk('STK_VOL_PERSIST', '600519.SH')], 'CN', '2026-05-20', {}),
                 out_dir=tmp_path)
    # D: 该信号未触发 → latest.json 不含它
    write_output('CN', '2026-05-21', universe_size=10,
                 signals=reconcile([], 'CN', '2026-05-21', load_previous('CN', tmp_path)),
                 out_dir=tmp_path)
    # D+1: 又触发了
    prev = load_previous('CN', tmp_path)
    sigs = reconcile([_mk('STK_VOL_PERSIST', '600519.SH')], 'CN', '2026-05-22', prev)
    assert sigs[0].isNew is True
    assert sigs[0].firstTriggeredDate == '2026-05-22'  # 重置, 不是 2026-05-20


def test_different_subjects_independent(tmp_path):
    write_output('CN', '2026-05-22', universe_size=10,
                 signals=reconcile([_mk('STK_VOL_PERSIST', '600519.SH')], 'CN', '2026-05-22', {}),
                 out_dir=tmp_path)
    prev = load_previous('CN', tmp_path)
    sigs = reconcile([
        _mk('STK_VOL_PERSIST', '600519.SH'),   # 持续
        _mk('STK_VOL_PERSIST', '000001.SZ'),   # 新触发
    ], 'CN', '2026-05-23', prev)
    by_id = {s.subject.id: s for s in sigs}
    assert by_id['600519.SH'].isNew is False
    assert by_id['000001.SZ'].isNew is True


def test_sort_order(tmp_path):
    sigs = [
        _mk('STK_BREAKOUT', 'A.SH', level='opportunity', sev=90),
        _mk('STK_BREAKDOWN', 'B.SH', level='risk', sev=50),
        _mk('STK_VOL_PERSIST', 'C.SH', level='watch', sev=80),
        _mk('STK_STRENGTH', 'D.SH', level='opportunity', sev=95),
    ]
    s = sort_signals(sigs)
    levels = [x.level for x in s]
    assert levels == ['risk', 'watch', 'opportunity', 'opportunity']
    # 同 level 内 severity 降序
    assert s[2].severity == 95 and s[3].severity == 90


def test_id_sanitization():
    # 含空格/特殊符号 (e.g. sector 名 "电力设备") 不应破坏 id 格式
    # 中文+空格+斜杠归一为单 '_'; ASCII letters/digits/./-/_ 保留
    assert make_signal_id('CN', 'SEC_BURST', '2026-05-22', '电力 设备/A') == \
           'CN-2026-05-22-SEC_BURST-_A'


def test_latest_file_format(tmp_path):
    sigs = reconcile([_mk('STK_VOL_PERSIST', '600519.SH')], 'CN', '2026-05-22', {})
    write_output('CN', '2026-05-22', universe_size=10, signals=sigs, out_dir=tmp_path)
    payload = json.loads((tmp_path / 'cn_latest.json').read_text())
    assert payload['market'] == 'CN'
    assert payload['asOfDate'] == '2026-05-22'
    assert payload['universeSize'] == 10
    assert len(payload['signals']) == 1
    sig = payload['signals'][0]
    assert sig['subject']['id'] == '600519.SH'
    assert sig['firstTriggeredDate'] == '2026-05-22'
    assert sig['isNew'] is True
