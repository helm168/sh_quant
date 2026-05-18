#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
CONFIG_PATH = PROJECT_ROOT / 'config' / 'news_radar.json'
DB_PATH = PROJECT_ROOT / 'data_cache' / 'news' / 'news_radar.sqlite3'
TUSHARE_URL = 'https://api.tushare.pro'


SOURCE_NAMES = {
    'sina': '新浪财经',
    'wallstreetcn': '华尔街见闻',
    '10jqka': '同花顺',
    'eastmoney': '东方财富',
    'yuncaijing': '云财经',
    'fenghuang': '凤凰新闻',
    'jinrongjie': '金融界',
    'cls': '财联社',
    'yicai': '第一财经',
}


def load_dotenv_token() -> str:
    token = os.getenv('TUSHARE_TOKEN', '').strip()
    if token:
        return token

    env_path = PROJECT_ROOT / '.env'
    if not env_path.exists():
        return ''

    for line in env_path.read_text(encoding='utf-8', errors='ignore').splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        if key.strip() == 'TUSHARE_TOKEN':
            return value.strip().strip('"').strip("'")
    return ''


def load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding='utf-8'))


def init_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute(
        """
        create table if not exists articles (
            id integer primary key autoincrement,
            source text not null,
            news_time text not null,
            title text not null,
            content text not null,
            norm_hash text not null unique,
            sim_key text not null,
            score integer not null,
            pushed integer not null default 0,
            created_at text not null
        )
        """
    )
    con.execute(
        """
        create table if not exists meta (
            key text primary key,
            value text not null
        )
        """
    )
    con.execute('create index if not exists idx_articles_time on articles(news_time)')
    con.execute('create index if not exists idx_articles_sim on articles(sim_key)')
    return con


def sources_for_run(con: sqlite3.Connection, config: dict) -> list[str]:
    sources = list(config['sources'])
    if not sources:
        return []

    per_run = max(1, int(config.get('sources_per_run', len(sources))))
    row = con.execute("select value from meta where key = 'source_cursor'").fetchone()
    cursor = int(row[0]) if row and row[0].isdigit() else 0

    selected = [sources[(cursor + idx) % len(sources)] for idx in range(min(per_run, len(sources)))]
    next_cursor = (cursor + len(selected)) % len(sources)
    con.execute(
        "insert into meta(key, value) values('source_cursor', ?) "
        'on conflict(key) do update set value = excluded.value',
        (str(next_cursor),),
    )
    return selected


def normalize(text: str) -> str:
    text = (text or '').lower()
    text = re.sub(r'https?://\\S+', '', text)
    text = re.sub(r'[\\s\\W_]+', '', text, flags=re.UNICODE)
    return text


def tokens(text: str) -> set[str]:
    norm = normalize(text)
    return {norm[i : i + 2] for i in range(max(0, len(norm) - 1))}


def hash_text(text: str) -> str:
    return hashlib.sha256(normalize(text).encode('utf-8')).hexdigest()


def sim_key(text: str) -> str:
    norm = normalize(text)
    if not norm:
        return ''
    return hashlib.sha1(norm[:80].encode('utf-8')).hexdigest()[:16]


def is_similar(con: sqlite3.Connection, text: str, minutes: int = 90) -> bool:
    now = dt.datetime.now()
    since = (now - dt.timedelta(minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S')
    current = tokens(text)
    if not current:
        return False

    rows = con.execute(
        'select title, content from articles where news_time >= ? order by id desc limit 250',
        (since,),
    ).fetchall()
    for title, content in rows:
        other = tokens(f'{title} {content}')
        if not other:
            continue
        overlap = len(current & other) / max(1, len(current | other))
        if overlap >= 0.72:
            return True
    return False


def score_item(config: dict, title: str, content: str, source: str) -> tuple[int, list[str]]:
    text = f'{title} {content}'
    score = 20
    reasons = []

    source_bonus = {
        'cls': 12,
        'yicai': 12,
        'wallstreetcn': 10,
        'eastmoney': 6,
        '10jqka': 6,
        'sina': 5,
    }.get(source, 0)
    score += source_bonus

    for group, words in config['high_value_keywords'].items():
        hits = [word for word in words if word and word.lower() in text.lower()]
        if hits:
            score += min(24, 8 + len(hits) * 4)
            reasons.append(f'{group}: {", ".join(hits[:3])}')

    low_hits = [word for word in config['low_value_keywords'] if word in text]
    if low_hits:
        score -= 25
        reasons.append(f'低价值: {", ".join(low_hits[:2])}')

    if len(content) >= 120:
        score += 6
    if re.search(r'据(央视|新华社|财联社|第一财经|华尔街见闻|证券时报|人民日报)', text):
        score += 8
        reasons.append('权威/主流来源引用')

    return max(0, min(100, score)), reasons[:3]


def tushare_news(token: str, source: str, start: str, end: str) -> list[dict]:
    payload = {
        'api_name': 'news',
        'token': token,
        'params': {'src': source, 'start_date': start, 'end_date': end},
        'fields': 'datetime,content,title,channels',
    }
    req = urllib.request.Request(
        TUSHARE_URL,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode('utf-8'))

    if data.get('code') not in (0, None):
        raise RuntimeError(f'{source}: {data.get("msg") or data}')

    fields = data.get('data', {}).get('fields', [])
    rows = data.get('data', {}).get('items', [])
    return [dict(zip(fields, row, strict=False)) for row in rows]


def format_push(items: list[dict]) -> str:
    if not items:
        return ''

    lines = ['财经快讯雷达']
    for item in items:
        source_name = SOURCE_NAMES.get(item['source'], item['source'])
        title = item['title'] or item['content'][:42]
        reason = '；'.join(item['reasons']) if item['reasons'] else '来源和关键词综合命中'
        lines.append('')
        lines.append(f'[{item["score"]}分] {title}')
        lines.append(f'来源：{source_name}｜时间：{item["news_time"]}')
        lines.append(f'原因：{reason}')
        if item['content'] and item['content'] != title:
            lines.append(item['content'][:240])
    return '\n'.join(lines)


def run_once(dry_run: bool = False) -> int:
    config = load_config()
    token = load_dotenv_token()
    if not token:
        print(
            '缺少 TUSHARE_TOKEN：请把 TUSHARE_TOKEN=你的token 写进 ~/.hermes/.env', file=sys.stderr
        )
        return 2

    con = init_db()
    now = dt.datetime.now()
    start = (now - dt.timedelta(minutes=config['lookback_minutes'])).strftime('%Y-%m-%d %H:%M:%S')
    end = now.strftime('%Y-%m-%d %H:%M:%S')

    candidates = []
    errors = []
    for source in sources_for_run(con, config):
        try:
            rows = tushare_news(token, source, start, end)
        except (urllib.error.URLError, TimeoutError, RuntimeError) as exc:
            errors.append(str(exc))
            continue

        for row in rows:
            title = (row.get('title') or '').strip()
            content = (row.get('content') or '').strip()
            news_time = (row.get('datetime') or end).strip()
            text = f'{title} {content}'.strip()
            if not text:
                continue
            norm_hash = hash_text(text)
            if con.execute('select 1 from articles where norm_hash = ?', (norm_hash,)).fetchone():
                continue
            if is_similar(con, text):
                continue

            score, reasons = score_item(config, title, content, source)
            con.execute(
                """
                insert or ignore into articles
                (source, news_time, title, content, norm_hash, sim_key, score, pushed, created_at)
                values (?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    source,
                    news_time,
                    title,
                    content,
                    norm_hash,
                    sim_key(text),
                    score,
                    now.isoformat(timespec='seconds'),
                ),
            )
            if score >= config['min_score']:
                candidates.append(
                    {
                        'source': source,
                        'news_time': news_time,
                        'title': title,
                        'content': content,
                        'score': score,
                        'reasons': reasons,
                        'norm_hash': norm_hash,
                    }
                )

    candidates.sort(key=lambda x: (x['score'], x['news_time']), reverse=True)
    selected = candidates[: config['max_push_items']]

    if selected and not dry_run:
        for item in selected:
            con.execute('update articles set pushed = 1 where norm_hash = ?', (item['norm_hash'],))
    con.commit()

    message = format_push(selected)
    if message:
        print(message)
    elif dry_run and errors:
        print('没有可推送新闻。抓取错误：' + ' | '.join(errors[:3]))

    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dry-run', action='store_true', help='print diagnostics even when no push is produced'
    )
    args = parser.parse_args()
    return run_once(dry_run=args.dry_run)


if __name__ == '__main__':
    raise SystemExit(main())
