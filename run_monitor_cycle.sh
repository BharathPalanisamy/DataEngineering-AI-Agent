#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
LOG_DIR="/Users/bharathpalanisamy/Library/Logs/AI_agent_project"
LOG_FILE="$LOG_DIR/monitor.log"
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

mkdir -p "$LOG_DIR"

{
  echo "============================================================"
  echo "Monitoring cycle started at: $TIMESTAMP"
  echo "============================================================"

  cd "$PROJECT_DIR"
  docker start dcp_postgres >/dev/null 2>&1 || true

  echo "[1/4] Running ingestion..."
  "$PYTHON_BIN" ingest/ingest.py

  echo "[2/4] Running schema drift check..."
  "$PYTHON_BIN" control_plane/schema_drift_check.py

  echo "[3/4] Generating drift report..."
  "$PYTHON_BIN" control_plane/generate_drift_report.py

  echo "[4/4] Running monitoring agent summary..."
  "$PYTHON_BIN" orchestrator/agent_runner.py

  echo "Monitoring cycle completed successfully."
  echo
} | tee -a "$LOG_FILE"
