#!/bin/bash
# ============================================
# 每日 08:30 早间链路
# 1. 先执行生产库存/货件同步
# 2. 仅当库存同步成功后，执行最新采购预测/颜色映射算法
# 3. 最终业务21列面料预估表写入飞书“面料预估明细”并导出Excel
#
# 支持“生产基础任务在 /opt/apps/pythondata，预测新版本在独立 worktree”部署。
# ============================================

set -euo pipefail

INVENTORY_PROJECT_DIR="${INVENTORY_PROJECT_DIR:-/opt/apps/pythondata}"
FORECAST_PROJECT_DIR="${FORECAST_PROJECT_DIR:-${PROJECT_DIR:-/opt/apps/pythondata}}"
VENV_DIR="${VENV_DIR:-/opt/apps/pythondata/venv}"
LOG_DIR="${LOG_DIR:-/opt/apps/pythondata/logs}"
MORNING_LOG="${MORNING_LOG:-$LOG_DIR/cron_morning_inventory_forecast.log}"

mkdir -p "$LOG_DIR"
cd "$FORECAST_PROJECT_DIR"

START_TS=$(date +%s)
echo "===================================" >> "$MORNING_LOG"
echo "早间库存+预测链路开始: $(date '+%Y-%m-%d %H:%M:%S')" >> "$MORNING_LOG"
echo "库存项目: $INVENTORY_PROJECT_DIR" >> "$MORNING_LOG"
echo "预测项目: $FORECAST_PROJECT_DIR" >> "$MORNING_LOG"

# 最新颜色体系映射项目路径；A/B 仍只按当前 SKU 自身显式标签识别。
export LX_PRODUCT_M_HOME="${LX_PRODUCT_M_HOME:-/opt/apps/lx-product-m}"

# 1. 库存同步。复用现有 inventory lock；失败/锁占用时立即停止，
# 不允许拿旧库存继续生成当天最终预测。
echo "[1/2] 执行08:30库存同步..." >> "$MORNING_LOG"
/usr/bin/flock -n /tmp/inventory_sync.lock \
    /bin/bash "$INVENTORY_PROJECT_DIR/scripts/run_inventory_sync.sh" \
    >> "$MORNING_LOG" 2>&1
echo "✓ 08:30库存同步完成" >> "$MORNING_LOG"

# 2. 最新预测 + SPU人工颜色映射 + 最终飞书21列表。
echo "[2/2] 执行最新采购预测与面料颜色算法..." >> "$MORNING_LOG"
PROJECT_DIR="$FORECAST_PROJECT_DIR" \
VENV_DIR="$VENV_DIR" \
LOG_DIR="$LOG_DIR" \
/bin/bash "$FORECAST_PROJECT_DIR/scripts/run_procurement_pipeline.sh" \
    >> "$MORNING_LOG" 2>&1
echo "✓ 最新算法及飞书面料预估写入完成" >> "$MORNING_LOG"

END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))
echo "早间库存+预测链路结束: $(date '+%Y-%m-%d %H:%M:%S')，耗时 ${ELAPSED}s" >> "$MORNING_LOG"
echo "===================================" >> "$MORNING_LOG"
