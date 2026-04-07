#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "[run] installing requirements if needed"
"$PYTHON_BIN" -m pip install --user -r requirements.txt

echo "[run] starting bot"
exec "$PYTHON_BIN" start.py
