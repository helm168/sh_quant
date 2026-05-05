"""按 config/themes.yaml 把所有主题成分股的「原始日线 + 复权因子」拉到 data_cache/stocks/。

依赖：tushare / pandas / pyarrow / python-dotenv / PyYAML（都在 requirements.txt 里）
环境：项目根 .env 里需要 TUSHARE_TOKEN

复权策略
--------
缓存 raw OHLCV + adj_factor，**不在拉取时复权**。读取时由
`utils/data.py:load_daily(adj='qfq'|'hfq'|None)` 按需计算。理由：

    - qfq 是动态序列（以最近一日为基准），缓存后历史值会随时间漂移，
      导致回测 reproducibility 失效。
    - 存 raw + adj_factor 后所有复权方式可在 ~5 行内推导：
        qfq_close = close * adj_factor / adj_factor.iloc[-1]
        hfq_close = close * adj_factor / adj_factor.iloc[0]

用法（先 source .venv/bin/activate，在项目根目录执行）：

    python scripts/pull_theme_stocks.py
    python scripts/pull_theme_stocks.py --start 20180101 --end 20251231
    python scripts/pull_theme_stocks.py --themes ai_compute,ev_battery
    python scripts/pull_theme_stocks.py --force                  # 已缓存的也重拉

输出：
    data_cache/stocks/<ts_code>.parquet
    列：trade_date / ts_code / open / high / low / close / pre_close
        / change / pct_chg / vol / amount / adj_factor
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

# 当前缓存 schema 必须包含的列（缺任一列视为 stale，触发重拉）
REQUIRED_COLUMNS = ("trade_date", "open", "close", "adj_factor")

PERMISSION_KEYWORDS = ("40203", "权限", "积分", "permission")


def is_stale(path: Path) -> bool:
    """缓存 parquet 是否需要重拉。

    旧版（commit be6f66f 之前）只存 qfq 调整后的价格，没有 adj_factor 列；
    新版必须有 adj_factor。schema 检查比文件 mtime 更可靠，未来加列也兼容。
    """
    try:
        import pyarrow.parquet as pq  # noqa: WPS433
        names = set(pq.read_schema(path).names)
    except Exception:
        return True   # 损坏 / 无法读取 → 重拉
    return any(c not in names for c in REQUIRED_COLUMNS)


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


def fetch_one(pro, ts_code: str, start: str, end: str) -> pd.DataFrame:
    """拉 raw daily + adj_factor，merge 后返回。权限/积分错误抛 PermissionError_。

    返回列：trade_date, ts_code, open, high, low, close, pre_close,
            change, pct_chg, vol, amount, adj_factor
    """
    try:
        daily = pro.daily(ts_code=ts_code, start_date=start, end_date=end)
    except Exception as e:  # noqa: BLE001
        if any(s in str(e) for s in PERMISSION_KEYWORDS):
            raise PermissionError_(str(e)) from e
        raise
    if daily is None or daily.empty:
        return pd.DataFrame()

    try:
        adj = pro.adj_factor(ts_code=ts_code, start_date=start, end_date=end)
    except Exception as e:  # noqa: BLE001
        if any(s in str(e) for s in PERMISSION_KEYWORDS):
            raise PermissionError_(str(e)) from e
        # adj_factor 偶发失败不致命：退化为不复权（adj_factor 全 1）
        adj = None

    daily = daily.sort_values("trade_date").reset_index(drop=True)
    if adj is not None and not adj.empty:
        adj = adj[["trade_date", "adj_factor"]].drop_duplicates("trade_date")
        daily = daily.merge(adj, on="trade_date", how="left")
        # 边界小填补：缺失的 adj_factor 按时间临近 ffill/bfill；都失败则填 1
        daily["adj_factor"] = daily["adj_factor"].ffill().bfill().fillna(1.0)
    else:
        daily["adj_factor"] = 1.0

    daily["trade_date"] = pd.to_datetime(daily["trade_date"], format="%Y%m%d")
    return daily


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20150101")
    ap.add_argument("--end",   default="20251231")
    ap.add_argument("--sleep", type=float, default=0.3,
                    help="每次接口调用之间的 sleep 秒数；本脚本 1 只股票发 2 次接口")
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
    pro = ts.pro_api()

    only = [t.strip() for t in args.themes.split(",") if t.strip()] or None
    themes = load_themes()
    universe = collect_universe(themes, only)
    n = len(universe)
    if n == 0:
        sys.exit("universe 为空，检查 themes.yaml 或 --themes 参数。")
    print(f"{len(themes)} 个主题, {n} 只去重股票  →  {CACHE_DIR.relative_to(PROJECT_ROOT)}/")
    print("拉 raw daily + adj_factor，复权由 utils/data.py 在读取时按需计算\n")

    failed: list[tuple[str, str, str]] = []
    stale_count = 0
    width = len(str(n))
    for i, (code, name) in enumerate(universe, 1):
        out = CACHE_DIR / f"{code}.parquet"
        if out.exists() and not args.force:
            if is_stale(out):
                stale_count += 1
                print(f"  [{i:>{width}}/{n}] stale {code} {name}  (旧 schema，重拉)")
            else:
                print(f"  [{i:>{width}}/{n}] skip  {code} {name}  (已缓存)")
                continue
        try:
            df = fetch_one(pro, code, args.start, args.end)
            if df.empty:
                print(f"  [{i:>{width}}/{n}] empty {code} {name}")
                failed.append((code, name, "empty"))
            else:
                df.to_parquet(out, index=False)
                af_first, af_last = df["adj_factor"].iloc[0], df["adj_factor"].iloc[-1]
                af_marker = "" if af_first == af_last == 1.0 else f", adj={af_first:.3f}→{af_last:.3f}"
                print(f"  [{i:>{width}}/{n}] ok    {code} {name}  ({len(df)} rows{af_marker})")
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
    if stale_count:
        print(f"识别并升级了 {stale_count} 个旧 schema 缓存。")
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
