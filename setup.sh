#!/usr/bin/env bash
# 一键 bootstrap：用 uv 在项目外创建 venv，并让依赖尽量链接 uv 全局 cache
# 用法（在 sh_quant 目录下）：
#   bash setup.sh
# 或：
#   make setup
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# 1) uv 是唯一安装入口：Python 下载、wheel cache、link-mode 都交给 uv 管。
if ! command -v uv >/dev/null 2>&1; then
  echo "[setup] 没找到 uv，请先安装 uv: https://docs.astral.sh/uv/" >&2
  exit 1
fi
echo "[setup] using $(uv --version) at $(command -v uv)"

# 2) venv 默认放在项目外，避免 Codex workspace 复制大量 site-packages。
#    项目根的 .venv 只是一个 symlink，兼容现有 .venv/bin/python 用法。
VENV_DIR="${SH_QUANT_VENV:-$HOME/.cache/uv-venvs/sh_quant}"
PYTHON_VERSION="${SH_QUANT_PYTHON:-3.13}"

if [ -e ".venv" ] && [ ! -L ".venv" ]; then
  echo "[setup] .venv 当前是真实目录，不会自动删除。" >&2
  echo "[setup] 如要迁移到共享 uv venv，请先手动执行：mv .venv .venv.local" >&2
  echo "[setup] 然后重新运行：bash setup.sh" >&2
  exit 1
fi

echo "[setup] venv: $VENV_DIR"
uv venv "$VENV_DIR" --python "$PYTHON_VERSION" --prompt sh_quant --allow-existing
ln -sfn "$VENV_DIR" .venv

# 3) 装依赖。symlink link-mode 会尽量把包链接到 uv 全局 cache，减少重复拷贝。
uv pip install --python "$VENV_DIR/bin/python" --link-mode symlink -r requirements.txt -e .

# 4) 注册 Jupyter kernel，方便在 notebook 里选这个环境
"$VENV_DIR/bin/python" -m ipykernel install --user --name sh_quant --display-name "Python (sh_quant)" || true

# 5) 简单自检
"$VENV_DIR/bin/python" - <<'PY'
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
