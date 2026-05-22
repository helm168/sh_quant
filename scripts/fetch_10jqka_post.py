"""抓 10jqka 同花顺帖子分享链接正文 → 标准 stdout 文本或原始 JSON。

为什么需要
    10jqka 上的研报转述（大摩拆解 XX / 海外大行评级 / 机构调研纪要）是更新
    themes.yaml / core_kpi.yaml 的研究输入。本工具**只搬字节** —— 提炼 insight
    仍要人（或 LLM）读完想清，再回头改 yaml。

API 发现（已验证、无需登录）
    分享链接 https://t.10jqka.com.cn/m/post/articleShare/articleshare.html?pid=<PID>
    是 Vue SPA 壳；真后端 JSON：
        GET https://t.10jqka.com.cn/m/post/getPostShareData/?pid=<PID>
        → {errorCode:0, result:{data:{post:{title, content(HTML), ...}}}}

依赖：requests

用法（在项目根，激活 venv 后）
    python scripts/fetch_10jqka_post.py 629029470
    python scripts/fetch_10jqka_post.py 'https://t.10jqka.com.cn/m/post/articleShare/...'
    python scripts/fetch_10jqka_post.py 629029470 --json > article.json   # 喂 LLM
    python scripts/fetch_10jqka_post.py 629029470 629030000               # 多 pid
"""

from __future__ import annotations

import argparse
import json
import re
import sys

import requests

API = 'https://t.10jqka.com.cn/m/post/getPostShareData/'
HEADERS = {
    'User-Agent': ('Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) '
                   'AppleWebKit/605.1.15'),
}
TIMEOUT = 15


def _pid_from(arg: str) -> str:
    """接受裸 pid 或分享 URL，返回 pid 字符串。"""
    if arg.isdigit():
        return arg
    m = re.search(r'[?&]pid=(\d+)', arg)
    if not m:
        raise ValueError(f'从 {arg!r} 取不到 pid（既不是纯数字也找不到 ?pid=）')
    return m.group(1)


def fetch_post(pid: str) -> dict:
    """打 getPostShareData，返回 post 字段（含 title/content 等）。"""
    r = requests.get(API, params={'pid': pid}, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    if j.get('errorCode') != 0:
        raise RuntimeError(f'pid={pid} 接口错误: errorCode={j.get("errorCode")} '
                           f'msg={j.get("errorMsg")!r}')
    return j['result']['data']['post']


def _clean_text(html: str) -> str:
    """去 HTML 标签 + 收拢空白。"""
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', html or '')).strip()


def render(pid: str, post: dict) -> str:
    """文本输出：标题 + 元数据 + 正文。"""
    out = ['━' * 60, f'  {post.get("title", "(无标题)")}', f'  [pid] {pid}']
    for k in ('user_name', 'create_time', 'ctime', 'time'):
        v = post.get(k)
        if v:
            out.append(f'  [{k}] {v}')
    out.append('─' * 60)
    out.append(_clean_text(post.get('content', '')))
    return '\n'.join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description='抓 10jqka 帖子分享链接正文')
    parser.add_argument('targets', nargs='+', help='pid 或分享 URL，可多个')
    parser.add_argument('--json', action='store_true', help='输出原始 post JSON 而非文本')
    args = parser.parse_args()

    failed: list[str] = []
    for t in args.targets:
        try:
            pid = _pid_from(t)
            post = fetch_post(pid)
            if args.json:
                print(json.dumps(post, ensure_ascii=False, indent=2))
            else:
                print(render(pid, post))
                print()
        except Exception as e:  # noqa: BLE001
            print(f'  ✗ {t}: {e!r}', file=sys.stderr)
            failed.append(t)
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
