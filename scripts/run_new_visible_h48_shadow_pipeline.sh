#!/bin/bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/opt/apps/pythondata}"
PYTHON="${PYTHON:-$PROJECT_DIR/venv-ml/bin/python}"
LOG_DIR="${LOG_DIR:-$PROJECT_DIR/logs}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/new_visible_h48_shadow_pipeline.log}"

cd "$PROJECT_DIR"
mkdir -p "$LOG_DIR"

run_step() {
  local name="$1"
  shift
  echo "[$(date '+%F %T')] START $name" | tee -a "$LOG_FILE"
  "$@" 2>&1 | tee -a "$LOG_FILE"
  echo "[$(date '+%F %T')] OK    $name" | tee -a "$LOG_FILE"
}

run_step "live_core"   "$PYTHON" scripts/materialize_new_visible_live_core.py

run_step "h48_prediction"   "$PYTHON" scripts/score_new_visible_h48_shadow.py

run_step "inventory_position"   "$PYTHON" scripts/materialize_new_visible_inventory_position_shadow.py

run_step "leadtime_risk"   "$PYTHON" scripts/build_new_visible_h48_leadtime_risk_shadow.py

run_step "h60_prediction"   "$PYTHON" scripts/score_new_visible_h60_shadow.py

run_step "horizon_monotonicity"   "$PYTHON" scripts/audit_new_visible_live_horizon_monotonicity.py

run_step "h60_inventory_coverage"   "$PYTHON" scripts/build_new_visible_h60_inventory_coverage_shadow.py

run_step "h60_fabric_classification"   "$PYTHON" scripts/classify_new_visible_h60_fabric_shadow.py

run_step "procurement_action_queue"   "$PYTHON" scripts/build_new_visible_procurement_action_shadow.py

run_step "procurement_action_export"   "$PYTHON" scripts/export_new_visible_procurement_action_shadow.py

echo "[$(date '+%F %T')] NEW_VISIBLE H48/H60 shadow pipeline complete" | tee -a "$LOG_FILE"
