#!/bin/bash
# ============================================
# 采购建议流水线
# 执行顺序：
#   1. 系统预测 vs 运营预计 对比表（generate_forecast_comparison）
#   2. 建议下单量 + 面料用量表（generate_procurement_report）
#   3. 导出 Excel 报告（export_procurement_excel）
# 失败时发飞书告警
# ============================================

PROJECT_DIR="/opt/apps/pythondata"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON="$VENV_DIR/bin/python"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/cron_procurement_pipeline.log"

cd "$PROJECT_DIR" || exit 1
source "$VENV_DIR/bin/activate" || exit 1
mkdir -p "$LOG_DIR"

START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
echo "===================================" >> "$LOG_FILE"
echo "开始时间: $START_TIME" >> "$LOG_FILE"

# ── 飞书告警函数 ──────────────────────────────────────────────────────────
send_feishu_error() {
    local step="$1"
    local exit_code="$2"
    $PYTHON "$PROJECT_DIR/scripts/notify_feishu.py" \
        --task "采购建议流水线" \
        --status "failed" \
        --detail "步骤 $step 失败，退出码: $exit_code，请查看日志 $LOG_FILE" \
        2>/dev/null || true
}

send_feishu_success() {
    local elapsed="$1"
    $PYTHON "$PROJECT_DIR/scripts/notify_feishu.py" \
        --task "采购建议流水线" \
        --status "success" \
        --detail "三步全部完成，Excel 已更新" \
        --elapsed "${elapsed}s" \
        2>/dev/null || true
}

# ── Step1：生成预测对比表 ─────────────────────────────────────────────────
echo "[1/3] generate_forecast_comparison..." >> "$LOG_FILE"
$PYTHON -m jobs.feishu.generate_forecast_comparison >> "$LOG_FILE" 2>&1
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "✗ Step1 失败 (退出码: $EXIT_CODE)" >> "$LOG_FILE"
    send_feishu_error "generate_forecast_comparison" "$EXIT_CODE"
    exit 1
fi
echo "✓ Step1 完成" >> "$LOG_FILE"

# ── Step2：生成建议下单量 + 面料用量 ─────────────────────────────────────
echo "[2/3] generate_procurement_report..." >> "$LOG_FILE"
$PYTHON -m jobs.feishu.generate_procurement_report >> "$LOG_FILE" 2>&1
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "✗ Step2 失败 (退出码: $EXIT_CODE)" >> "$LOG_FILE"
    send_feishu_error "generate_procurement_report" "$EXIT_CODE"
    exit 1
fi
echo "✓ Step2 完成" >> "$LOG_FILE"

# ── Step3：导出 Excel ─────────────────────────────────────────────────────
echo "[3/3] export_procurement_excel..." >> "$LOG_FILE"
$PYTHON -m jobs.feishu.export_procurement_excel >> "$LOG_FILE" 2>&1
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "✗ Step3 失败 (退出码: $EXIT_CODE)" >> "$LOG_FILE"
    send_feishu_error "export_procurement_excel" "$EXIT_CODE"
    exit 1
fi
echo "✓ Step3 完成" >> "$LOG_FILE"

# ── 完成 ─────────────────────────────────────────────────────────────────
END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
START_TS=$(date -d "$START_TIME" +%s 2>/dev/null || date -j -f '%Y-%m-%d %H:%M:%S' "$START_TIME" +%s 2>/dev/null)
END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))

echo "结束时间: $END_TIME，耗时: ${ELAPSED}s" >> "$LOG_FILE"
echo "===================================" >> "$LOG_FILE"

send_feishu_success "$ELAPSED"
