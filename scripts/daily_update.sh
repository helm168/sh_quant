#!/usr/bin/env bash
# 日更 + 数据新鲜度自检 wrapper.
#
# launchd (com.helm.shquant.daily) 调本脚本而非直接调 update_daily.py:
# 日更写完立即跑 validate_freshness, 趁 fetched_at 刚落盘 + mtime 没被冲, 检出
# "盘中半值"污染 (2026-05-26 踩坑: 美股盘中跑过一次 → 当天半日 OHLCV 被 fresh-skip
# 卡死, 当日终值永不补). validator --quiet 下只打 FLAG + 汇总, 平时日志干净.
#
# validator 的 FLAG (退出码 1) **不**让整个 cron 标红 —— 日更数据已经写了, 检测
# 失败不该掩盖日更成功; FLAG 行留在日志里, 人工 / 后续通知处理. wrapper 最终以
# 日更退出码为准.
#
# 用法 (plist ProgramArguments 传日更参数, 透传给 update_daily.py):
#   bash scripts/daily_update.sh --market us,cn
set -uo pipefail

PROJECT_ROOT="/Users/helm/Documents/Code/sh_quant"
PY="$PROJECT_ROOT/.venv/bin/python"
cd "$PROJECT_ROOT" || exit 1

echo "=== [wrapper] update_daily $* @ $(date '+%Y-%m-%d %H:%M:%S') ==="
"$PY" scripts/update_daily.py "$@"
daily_rc=$?

echo "=== [wrapper] validate_freshness @ $(date '+%Y-%m-%d %H:%M:%S') ==="
"$PY" scripts/validate_freshness.py --quiet
val_rc=$?
if [ "$val_rc" -ne 0 ]; then
    echo "!!! [wrapper] validate_freshness 检出 FLAG (盘中半值)! 见上方 FLAG 行."
    echo "!!! 修复: $PY scripts/update_daily.py --tickers <FLAG列表> --force"
fi

# 以日更退出码为准 (validator 不阻塞 cron)
exit "$daily_rc"
