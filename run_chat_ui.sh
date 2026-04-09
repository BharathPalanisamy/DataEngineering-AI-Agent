#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python environment not found at $PYTHON_BIN"
  exit 1
fi

echo "Starting Data Control Plane chat UI on http://127.0.0.1:8501"
exec "$PYTHON_BIN" -m mcp_server.chat_ui
