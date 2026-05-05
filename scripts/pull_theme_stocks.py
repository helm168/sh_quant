"""按 config/themes.yaml 把所有主题成分股的日线（前复权）拉到 data_cache/stocks/。

依赖：tushare / pandas / pyarrow / python-dotenv / PyYAML（都在 requirements.txt 里）
环境：项目根 .env 里需要 TUSHARE_TOKEN

用法（先 source .venv/bin/activate，在项目根目录执行）：

    python scripts/pull_theme_stocks.py
    python scripts/pull_theme_stocks.py --start 20180101 --end 20251231
    python scripts/pull_theme_stocks.py --adj hfq                # 后复权
    python scripts/pull_theme_stocks.py --adj None               # 不复权
    python scripts/pull_theme_stocks.py --themes ai_compute,ev_battery
    python scripts/pull_theme_stocks.py --force                  # 已缓存的也重拉

输出：
    data_cache/stocks/<ts_code>.parquet      每只股票一份日线
    （多个主题包含同一只股票时只拉一次，按 ts_code 去重）
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
THEMES_YAML = PROJECT_ROOT / "config" / "themes.yaml"
CACHE_DIR = PROJECT_ROOT / "data_cache" / "stocks"

PERMISSION_KEYWORDS = ("40203", "权限", "积分", "permission")


class PermissionError_(RuntimeError):
    """tushare 权限/积分错误，触发后立即终止。"""


def load_token() -> str:
    try:
        from dotenv import load_dotenv  # noqa: WPS433
    except ImportError:
        sys.exit("python-dotenv 没装。先 `bash setup.sh`。")
    load_dotenv(PROJECT_ROOT / ".env")
    token = os.getenv("TUSHARE_TOKEN")
    if not token or token == "your_tushare_token_here":
        sys.exit("TUSHARE_TOKEN not found in .env。cp .env.example .env，再填你的真 token。")
    return token


def load_themes() -> dict:
    try:
        import yaml  # noqa: WPS433
    except ImportError:
        sys.exit("PyYAML 没装。`pip install PyYAML` 或重跑 `bash setup.sh`。")
    if not THEMES_YAML.exists():
        sys.exit(f"找不到 {THEMES_YAML.relative_to(PROJECT_ROOT)}。")
    with open(THEMES_YAML, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not data or "themes" not in data:
        sys.exit("themes.yaml 顶层缺少 `themes` 字段。")
    return data["themes"]


def collect_universe(themes: dict, only: list[str] | None) -> list[tuple[str, str]]:
    """返回 [(ts_code, name), ...]，按 ts_code 去重。

    主题/subtrack 元数据走 config/themes.yaml 单源真相，不写到 parquet 里——
    研究时按需 yaml.safe_load 读出来 filter 即可，避免 parquet 与 yaml 漂移。
    """
    pool: dict[str, str] = {}
    for theme_id, t in themes.items():
        if only and theme_id not in only:
            continue
        for s in t.get("stocks", []) or []:
            code = s.get("code")
            if code and code not in pool:
                pool[code] = s.get("name", "")
    return list(pool.items())


def fetch_one(ts_module, ts_code: str, start: str, end: str, adj: str | None) -> pd.DataFrame:
    """ts.pro_bar 拉个股日线（默认 qfq 前复权）。权限错误抛 PermissionError_。"""
    try:
        df = ts_module.pro_bar(
            ts_code=ts_code, adj=adj, start_date=start, end_date=end
        )
    except Exception as e:  # noqa: BLE001
        if any(s in str(e) for s in PERMISSION_KEYWORDS):
            raise PermissionError_(str(e)) from e
        raise
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20150101")
    ap.add_argument("--end",   default="20251231")
    ap.add_argument("--adj",   default="qfq", help="复权方式：qfq / hfq / None")
    ap.add_argument("--sleep", type=float, default=0.3)
    ap.add_argument("--force", action="store_true", help="忽略已有缓存，全部重拉")
    ap.add_argument("--themes", default="",
                    help="只拉指定主题（逗号分隔），如 ai_compute,ev_battery")
    args = ap.parse_args()

    try:
        import tushare as ts
    except ImportError:
        sys.exit("tushare 没装。先 `bash setup.sh`。")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    ts.set_token(load_token())
    pro = ts.pro_api()  # 仅为触发 token 校验  # noqa: F841

    only = [t.strip() for t in args.themes.split(",") if t.strip()] or None
    themes = load_themes()
    universe = collect_universe(themes, only)
    n = len(universe)
    if n == 0:
        sys.exit("universe 为空，检查 themes.yaml 或 --themes 参数。")
    print(f"{len(themes)} 个主题, {n} 只去重股票  →  {CACHE_DIR.relative_to(PROJECT_ROOT)}/")

    adj = None if args.adj.lower() == "none" else args.adj.lower()
    failed: list[tuple[str, str, str]] = []
    width = len(str(n))
    for i, (code, name) in enumerate(universe, 1):
        out = CACHE_DIR / f"{code}.parquet"
        if out.exists() and not args.force:
            print(f"  [{i:>{width}}/{n}] skip  {code} {name}  (已缓存)")
            continue
        try:
            df = fetch_one(ts, code, args.start, args.end, adj)
            if df.empty:
                print(f"  [{i:>{width}}/{n}] empty {code} {name}")
                failed.append((code, name, "empty"))
            else:
                df.to_parquet(out, index=False)
                print(f"  [{i:>{width}}/{n}] ok    {code} {name}  ({len(df)} rows, adj={adj})")
        except PermissionError_ as e:
            sys.exit(
                f"\n[{i}/{n}] 权限/积分不足: {code} {name}"
                f"\n  -> {e}"
                f"\n后续股票大概率同样失败。先解决权限问题再重跑。"
            )
        except Exception as e:  # noqa: BLE001
            print(f"  [{i:>{width}}/{n}] FAIL  {code} {name}  -> {e}")
            failed.append((code, name, str(e)))
        time.sleep(args.sleep)

    print()
    if failed:
        print(f"{len(failed)} 个失败/空：")
        for code, name, err in failed[:30]:
            print(f"  {code} {name}: {err}")
        if len(failed) > 30:
            print(f"  ... 还有 {len(failed) - 30} 个")
        sys.exit(1 if any(e != "empty" for _, _, e in failed) else 0)
    print("all done.")


if __name__ == "__main__":
    main()
