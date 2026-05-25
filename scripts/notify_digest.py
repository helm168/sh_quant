"""读 signals/*_latest.json → 拼飞书 interactive 卡片 → POST webhook.

PRD §2 v2 候选, 但实现极轻 (~200 行, stdlib only) 所以提到 v1 一起做.
"信号落盘 ≠ 通知到我", 这层负责 "推到面前".

设计要点
────────
1. **只推新 isNew=true** (PRD §6.1 边沿触发) —— 复读 = 警报疲劳.
2. **跑批不阻塞 cron** —— webhook 挂 / 飞书 502 都不让 launchd 标红, 但要
   在 stdout 留醒目错误 (cron log 看得到).
3. **token / webhook 缺就报错退出 exit 2**, 不静默 skip (per memory:
   feedback_surface_permission_failures).
4. **没新信号 = 不发**. 不要为了"今天跑过了"硬发一条"今日无新信号" ——
   消息越少越值钱.

用法
────
    source .venv/bin/activate
    python scripts/notify_digest.py                # 跑全配置市场
    python scripts/notify_digest.py --markets CN   # 只推 CN
    python scripts/notify_digest.py --dry-run      # 拼好卡片但不 POST, 打印 payload

依赖
────
    ~/.market_data/signals/<market>_latest.json    (pull_signals.py 产出)
    .env: FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/<uuid>
    config/signals.yaml 的 notify: 段

exit code
─────────
    0 — 推送成功 / 没新信号也算成功
    1 — 网络或飞书 API 报错 (会重试一次再判)
    2 — 配置 / webhook URL 缺失 (需人工干预)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from utils.signals_engine import _data_root  # noqa: E402
from utils.themes import get_codes  # noqa: E402

# ─────────── .env 加载 (跟 pull_macro.py 同套路) ───────────


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        sys.exit('[ABORT] python-dotenv 没装。先 `bash setup.sh`.')

    # 优先 worktree 自己的 .env, 否则 fallback 到 git common-dir 的 .env
    if (PROJECT_ROOT / '.env').exists():
        load_dotenv(PROJECT_ROOT / '.env')
        return
    try:
        common = subprocess.check_output(
            ['git', 'rev-parse', '--path-format=absolute', '--git-common-dir'],
            cwd=PROJECT_ROOT, text=True,
        ).strip()
        main_env = Path(common).parent / '.env'
        if main_env.exists():
            load_dotenv(main_env)
    except Exception:  # noqa: BLE001
        pass


# ─────────── 卡片渲染 ───────────


_LEVEL_LABEL = {'risk': '🔴 风险', 'watch': '🟡 关注', 'opportunity': '🟢 机会'}


def _line_for_signal(s: dict, deeplink_template: str) -> str:
    """单条信号在飞书 markdown 里的一行."""
    subj = s['subject']
    name = subj.get('name') or subj['id']
    sev = s['severity']
    title = s['title']
    # subject 标识 — 个股带 ticker(name), 大盘只写 market, 板块写名字
    if subj['kind'] == 'stock':
        ident = f"**{subj['id']}** {name}"
        if deeplink_template:
            link = deeplink_template.format(ts_code=subj['id'], ticker=subj.get('ticker', ''))
            ident = f"[{ident}]({link})"
    elif subj['kind'] == 'sector':
        ident = f'**{name}**'
    else:
        ident = f'**{subj["id"]}**'
    return f'`{sev:>3}` {ident} — {title}'


def _resolve_filter_path() -> Path | None:
    """优先读 ~/.market_data/notify_filter.yaml (用户数据, 不入仓),
    fallback 到仓里的 config/notify_filter.yaml (模板/CI).

    个人持仓属于敏感数据 (跟 .env 同级), 永远不应该 commit. 仓里只放
    portfolio=[] 的模板; 真实持仓由用户 (或未来 WealthPilot UI) 写到
    ~/.market_data/notify_filter.yaml.
    """
    user_fp = _data_root() / 'notify_filter.yaml'
    if user_fp.exists():
        return user_fp
    repo_fp = PROJECT_ROOT / 'config' / 'notify_filter.yaml'
    if repo_fp.exists():
        return repo_fp
    return None


def _load_user_filter() -> dict:
    """读过滤器 yaml + 展开 opportunity_themes → universe set."""
    fp = _resolve_filter_path()
    if fp is None:
        # 没配过滤器时 = 全推 (跟 v1.0 行为一致, 不破坏老用户)
        return {'portfolio': set(), 'opp_universe': None,
                'push_market_risk': True, 'risk_portfolio_only': False,
                '_source': '(none)'}
    raw = yaml.safe_load(fp.read_text()) or {}
    portfolio = set(raw.get('portfolio') or [])
    opp_codes: set[str] = set()
    for theme_id in raw.get('opportunity_themes') or []:
        try:
            opp_codes.update(get_codes(theme_id))
        except KeyError:
            print(f'  [WARN] notify_filter.yaml: theme {theme_id!r} 在 themes.yaml 找不到, 跳过')
    return {
        'portfolio': portfolio,
        # None = 不过滤 universe (兼容); 空 set = 一条不推
        'opp_universe': opp_codes if opp_codes else None,
        'push_market_risk': bool(raw.get('push_market_risk', True)),
        'risk_portfolio_only': bool(raw.get('risk_portfolio_only', False)),
        '_source': str(fp),
    }


def _passes_user_filter(s: dict, uf: dict) -> bool:
    """按 portfolio / theme 过滤. 详见 config/notify_filter.yaml 顶部注释."""
    scope = s.get('scope')
    level = s.get('level')
    if scope in ('market', 'sector'):
        return level != 'risk' or uf['push_market_risk']
    # scope == 'stock'
    ts_code = s['subject']['id']
    in_port = ts_code in uf['portfolio']
    # opp_universe is None = 不过滤 (兼容旧用户); 否则要求落在主题内
    in_opp = True if uf['opp_universe'] is None else ts_code in uf['opp_universe']
    if level == 'risk':
        return in_port if uf['risk_portfolio_only'] else True
    if level == 'opportunity':
        return in_opp
    # level == 'watch' — portfolio OR opportunity
    return in_port or in_opp


def _filter_signals(payload: dict, cfg: dict, uf: dict) -> list[dict]:
    """按 notify config + 用户视角过滤; 返回该市场要推的 signal 列表 (已排序)."""
    out: list[dict] = []
    for s in payload.get('signals', []):
        if cfg['only_new'] and not s.get('isNew'):
            continue
        if s.get('severity', 0) < cfg['min_severity']:
            continue
        if not _passes_user_filter(s, uf):
            continue
        out.append(s)
    # signals 已按 level/severity 排好序 (pull_signals.py 落盘前 sort_signals)
    return out[:cfg['max_per_market']]


def _build_card(per_market: dict[str, tuple[dict, list[dict]]], deeplink_template: str) -> dict:
    """per_market = {market: (payload, filtered_signals)}; 返回飞书 card JSON."""
    # header 颜色按全局最高 level 取
    all_levels = {s['level'] for _, sigs in per_market.values() for s in sigs}
    if 'risk' in all_levels:
        template = 'red'
    elif 'watch' in all_levels:
        template = 'orange'
    else:
        template = 'green'

    total = sum(len(sigs) for _, sigs in per_market.values())
    as_of_dates = sorted({p['asOfDate'] for p, _ in per_market.values()})
    title = f'📊 sh_quant 信号 · {as_of_dates[-1]} · {total} 条新触发'

    elements: list[dict] = []
    for i, (market, (payload, sigs)) in enumerate(per_market.items()):
        if i > 0:
            elements.append({'tag': 'hr'})
        # 市场标题行
        elements.append({
            'tag': 'div',
            'text': {
                'tag': 'lark_md',
                'content': (
                    f'**{market}** · asOf {payload["asOfDate"]} · '
                    f'universe {payload["universeSize"]} · 推送 {len(sigs)} 条'
                ),
            },
        })
        # 按 level 分组, 同 level 内已排序
        by_level: dict[str, list[dict]] = {'risk': [], 'watch': [], 'opportunity': []}
        for s in sigs:
            by_level[s['level']].append(s)
        for level in ('risk', 'watch', 'opportunity'):
            block = by_level[level]
            if not block:
                continue
            md_lines = [_LEVEL_LABEL[level]]
            for s in block:
                md_lines.append('• ' + _line_for_signal(s, deeplink_template))
            elements.append({
                'tag': 'div',
                'text': {'tag': 'lark_md', 'content': '\n'.join(md_lines)},
            })

    return {
        'msg_type': 'interactive',
        'card': {
            'config': {'wide_screen_mode': True},
            'header': {
                'title': {'tag': 'plain_text', 'content': title},
                'template': template,
            },
            'elements': elements,
        },
    }


# ─────────── 投递 ───────────


def _post_feishu(webhook_url: str, payload: dict, timeout: int = 15) -> tuple[int, dict]:
    """POST 卡片. 返回 (http_status, response_body_json).

    飞书返回示例 (成功): {"code":0,"msg":"success","data":{}}
    失败时 code != 0, msg 是中文说明.
    """
    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    req = urllib.request.Request(
        webhook_url,
        data=body,
        headers={'Content-Type': 'application/json; charset=utf-8'},
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        status = resp.status
        text = resp.read().decode('utf-8', errors='replace')
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = {'raw': text}
    return status, data


def _post_with_retry(webhook_url: str, payload: dict, max_attempts: int = 2) -> int:
    """重试一次. 返回 0 = OK, 1 = 失败 (含飞书 code != 0)."""
    last_err = ''
    for attempt in range(1, max_attempts + 1):
        try:
            status, data = _post_feishu(webhook_url, payload)
            code = data.get('code')
            if status == 200 and code == 0:
                print(f'  [OK] 飞书推送成功 (attempt {attempt})')
                return 0
            last_err = f'http={status} code={code} msg={data.get("msg")!r}'
            # code != 0 多半是参数问题, 重试也没用
            if code is not None and code != 0:
                break
        except urllib.error.URLError as ex:
            last_err = f'URLError: {ex.reason}'
        except Exception as ex:  # noqa: BLE001
            last_err = f'{ex.__class__.__name__}: {ex}'
        if attempt < max_attempts:
            time.sleep(2)
    print(f'  [FAIL] 飞书推送失败: {last_err}')
    return 1


# ─────────── 主流程 ───────────


def main() -> int:
    p = argparse.ArgumentParser(description='飞书 digest 推送 (PRD §2 v2 候选)')
    p.add_argument('--markets', default='',
                   help='逗号分隔, 覆盖 yaml 配置 (e.g. CN,US)')
    p.add_argument('--dry-run', action='store_true',
                   help='拼卡片但不 POST, 打印 payload')
    args = p.parse_args()

    cfg_fp = PROJECT_ROOT / 'config' / 'signals.yaml'
    if not cfg_fp.exists():
        sys.exit(f'[ABORT] 缺 {cfg_fp}')
    full_cfg = yaml.safe_load(cfg_fp.read_text())
    notify_cfg = full_cfg.get('notify', {}).get('feishu', {})
    if not notify_cfg.get('enabled'):
        print('[SKIP] notify.feishu.enabled=false, 不推送')
        return 0

    markets = (
        [m.strip().upper() for m in args.markets.split(',') if m.strip()]
        or [m.upper() for m in notify_cfg['markets']]
    )

    user_filter = _load_user_filter()
    print(f'  [filter] src={user_filter["_source"]} '
          f'portfolio={len(user_filter["portfolio"])} '
          f'opp_universe={len(user_filter["opp_universe"]) if user_filter["opp_universe"] else "all"} '
          f'push_market_risk={user_filter["push_market_risk"]} '
          f'risk_portfolio_only={user_filter["risk_portfolio_only"]}')

    # 读 signals/*_latest.json
    signals_dir = _data_root() / full_cfg['engine']['out_subdir']
    per_market: dict[str, tuple[dict, list[dict]]] = {}
    for market in markets:
        fp = signals_dir / f'{market.lower()}_latest.json'
        if not fp.exists():
            print(f'  [{market}] {fp.name} 不存在, 跳过 (先跑 pull_signals.py)')
            continue
        try:
            payload = json.loads(fp.read_text())
        except json.JSONDecodeError as ex:
            print(f'  [{market}] {fp.name} 损坏 ({ex}), 跳过')
            continue
        sigs = _filter_signals(payload, notify_cfg, user_filter)
        print(f'  [{market}] total={len(payload.get("signals", []))} '
              f'→ kept={len(sigs)} (only_new={notify_cfg["only_new"]} '
              f'min_sev={notify_cfg["min_severity"]})')
        if sigs:
            per_market[market] = (payload, sigs)

    if not per_market:
        print('[OK] 没有新触发信号, 不发卡片 (这是好事)')
        return 0

    card = _build_card(per_market, notify_cfg.get('deeplink_template', ''))

    if args.dry_run:
        print('--- DRY RUN: payload ---')
        print(json.dumps(card, ensure_ascii=False, indent=2))
        return 0

    # 实跑时才读 webhook URL, dry-run 可以不配
    _load_env()
    webhook = os.getenv('FEISHU_WEBHOOK_URL', '').strip()
    if not webhook or 'open.feishu.cn' not in webhook:
        # 跟 memory:feedback_surface_permission_failures 一致: 配置缺失大声报
        print('[ABORT-CONFIG] FEISHU_WEBHOOK_URL 未配或格式异常.')
        print('  在 .env 加: FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/<uuid>')
        print('  飞书群 → 设置 → 群机器人 → 添加机器人 → 自定义机器人 → 拿 webhook')
        return 2

    return _post_with_retry(webhook, card)


if __name__ == '__main__':
    sys.exit(main())
