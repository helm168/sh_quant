#!/usr/bin/env bash
# 一键 bootstrap：在项目根目录创建 .venv 并安装 requirements.txt
# 用法（在 sh_quant 目录下）：
#   bash setup.sh
# 或：
#   make setup
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# 1) 选 python：优先 python3.11 / 3.12，其次 python3
PY=""
for c in python3.12 python3.11 python3.10 python3; do
  if command -v "$c" >/dev/null 2>&1; then PY="$c"; break; fi
done
if [ -z "$PY" ]; then
  echo "[setup] 没找到 python3，请先安装 Python (>=3.10)。" >&2
  exit 1
fi
echo "[setup] using $($PY --version) at $(command -v $PY)"

# 2) 建 venv
if [ ! -d ".venv" ]; then
  echo "[setup] creating .venv ..."
  "$PY" -m venv .venv
else
  echo "[setup] .venv already exists, reusing."
fi

# 3) 升级 pip + 装依赖
# shellcheck disable=SC1091
source .venv/bin/activate
python -m pip install --upgrade pip wheel setuptools
python -m pip install -r requirements.txt

# 4) 注册 Jupyter kernel，方便在 notebook 里选这个环境
python -m ipykernel install --user --name sh_quant --display-name "Python (sh_quant)" || true

# 5) 简单自检
python - <<'PY'
import importlib, sys
mods = ["numpy", "pandas", "pyarrow", "matplotlib", "tushare", "scipy", "statsmodels", "sklearn"]
print("python:", sys.version.split()[0])
for m in mods:
    try:
        v = importlib.import_module(m).__version__
        print(f"  ok  {m:<12} {v}")
    except Exception as e:
        print(f"  FAIL {m}: {e}")
PY

echo
echo "[setup] done. 激活环境：source .venv/bin/activate"
echo "[setup] 启动 notebook：jupyter lab"
