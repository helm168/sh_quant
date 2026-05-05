"""一次性拉申万一级行业指数 2015–2025 日线数据，落到 data_cache/sw_l1/。

依赖：tushare / pandas / pyarrow / python-dotenv（都在 requirements.txt 里）
环境：项目根 .env 里需要 TUSHARE_TOKEN

用法（先 source .venv/bin/activate，在项目根目录执行）：

    python scripts/pull_sw_l1.py
    python scripts/pull_sw_l1.py --start 20180101 --end 20251231
    python scripts/pull_sw_l1.py --src SW2014        # 用老版 28 个行业
    python scripts/pull_sw_l1.py --force             # 已缓存的也重拉

输出：
    data_cache/sw_l1/_industries.parquet      行业代码 → 行业名映射
    data_cache/sw_l1/801010.SI.parquet        每个行业一份日线
    ...
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = PROJECT_ROOT / "data_cache" / "sw_l1"


def load_token() -> str:
    """从项目根 .env 读 TUSHARE_TOKEN。"""
    try:
        from dotenv import load_dotenv  # noqa: WPS433
    except ImportError:
        sys.exit("python-dotenv 没装。先 `bash setup.sh` 或 `pip install python-dotenv`。")
    load_dotenv(PROJECT_ROOT / ".env")
    token = os.getenv("TUSHARE_TOKEN")
    if not token or token == "your_tushare_token_here":
        sys.exit("TUSHARE_TOKEN not found in .env. cp .env.example .env，再填你的真 token。")
    return token


PERMISSION_KEYWORDS = ("40203", "权限", "积分", "permission")


class PermissionError_(RuntimeError):
    """tushare 权限/积分相关错误，触发后立即终止，不继续后续行业。"""


def fetch_one(pro, ts_code: str, start: str, end: str) -> pd.DataFrame:
    """只走 sw_daily。权限/积分错误抛 PermissionError_，由调用方决定是否中止。"""
    try:
        df = pro.sw_daily(ts_code=ts_code, start_date=start, end_date=end)
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
    ap.add_argument("--start", default="20150101", help="起始日期 YYYYMMDD")
    ap.add_argument("--end",   default="20251231", help="结束日期 YYYYMMDD")
    ap.add_argument("--src",   default="SW2021", choices=["SW2014", "SW2021"],
                    help="申万版本（SW2021 31 个 / SW2014 28 个）")
    ap.add_argument("--sleep", type=float, default=0.3,
                    help="每次调用之间等的秒数（避开 tushare 速率限）")
    ap.add_argument("--force", action="store_true",
                    help="忽略已有缓存，全部重拉")
    args = ap.parse_args()

    try:
        import tushare as ts
    except ImportError:
        sys.exit("tushare 没装。先 `bash setup.sh`。")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    ts.set_token(load_token())
    pro = ts.pro_api()

    # 1) 拿 SW L1 行业列表
    classify = pro.index_classify(level="L1", src=args.src)
    if classify is None or classify.empty:
        sys.exit("index_classify 返回空，可能是权限或参数问题。")
    classify = classify[["index_code", "industry_name"]].rename(
        columns={"index_code": "ts_code"}
    )
    n = len(classify)
    print(f"[{args.src}] {n} 个一级行业")
    classify.to_parquet(CACHE_DIR / "_industries.parquet", index=False)

    # 2) 逐个拉日线
    failed: list[tuple[str, str, str]] = []
    for i, row in enumerate(classify.itertuples(index=False), 1):
        code = row.ts_code
        name = row.industry_name
        out = CACHE_DIR / f"{code}.parquet"
        if out.exists() and not args.force:
            print(f"  [{i:>2}/{n}] skip  {code} {name}  (已缓存)")
            continue
        try:
            df = fetch_one(pro, code, args.start, args.end)
            if df.empty:
                print(f"  [{i:>2}/{n}] empty {code} {name}")
                failed.append((code, name, "empty"))
            else:
                df["industry_name"] = name
                df.to_parquet(out, index=False)
                print(f"  [{i:>2}/{n}] ok    {code} {name}  ({len(df)} rows, via sw_daily)")
        except PermissionError_ as e:
            sys.exit(
                f"\n[{i}/{n}] 权限/积分不足: {code} {name}"
                f"\n  -> {e}"
                f"\n后续行业大概率同样失败。先解决权限问题再重跑。"
            )
        except Exception as e:  # noqa: BLE001
            print(f"  [{i:>2}/{n}] FAIL  {code} {name}  -> {e}")
            failed.append((code, name, str(e)))
        time.sleep(args.sleep)

    print()
    if failed:
        print(f"{len(failed)} 个失败：")
        for code, name, err in failed:
            print(f"  {code} {name}: {err}")
        sys.exit(1)
    print("all done.")


if __name__ == "__main__":
    main()
