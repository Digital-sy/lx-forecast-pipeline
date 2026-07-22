#!/bin/bash
# ============================================
# 采购建议核心流水线
# 1. 飞书运营预计下单量 → MySQL
# 2. 系统预测 vs 运营预计 → 预测对比表 / 预测对比表_SKU
# 3. 颜色体系建议下单量 + 面料汇总 + 面料详细预估 → MySQL / 飞书
# 4. 导出采购建议 Excel
#
# 任一步骤失败都会停止后续任务并发送飞书告警。
# ============================================

set -u

PROJECT_DIR="${PROJECT_DIR:-/opt/apps/pythondata}"
VENV_DIR="${VENV_DIR:-$PROJECT_DIR/venv}"
PYTHON="${PYTHON:-$VENV_DIR/bin/python}"
LOG_DIR="${LOG_DIR:-$PROJECT_DIR/logs}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/cron_procurement_pipeline.log}"

cd "$PROJECT_DIR" || {
    echo "项目目录不存在：$PROJECT_DIR" >&2
    exit 1
}

if [ ! -x "$PYTHON" ]; then
    echo "Python 解释器不存在或不可执行：$PYTHON" >&2
    exit 1
fi

mkdir -p "$LOG_DIR"
START_TS=$(date +%s)
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')

echo "===================================" >> "$LOG_FILE"
echo "开始时间: $START_TIME" >> "$LOG_FILE"

send_feishu_error() {
    local step="$1"
    local exit_code="$2"
    "$PYTHON" "$PROJECT_DIR/scripts/notify_feishu.py" \
        --task "采购建议流水线" \
        --status "failed" \
        --detail "步骤 ${step} 失败，退出码: ${exit_code}，请查看日志 ${LOG_FILE}" \
        2>/dev/null || true
}

send_feishu_success() {
    local elapsed="$1"
    "$PYTHON" "$PROJECT_DIR/scripts/notify_feishu.py" \
        --task "采购建议流水线" \
        --status "success" \
        --detail "四个业务步骤全部完成，预测、颜色体系采购建议、面料明细和 Excel 已更新" \
        --elapsed "${elapsed}s" \
        2>/dev/null || true
}

fail_step() {
    local step="$1"
    local exit_code="$2"
    echo "✗ ${step} 失败（退出码: ${exit_code}）" >> "$LOG_FILE"
    send_feishu_error "$step" "$exit_code"
    exit "$exit_code"
}

run_module() {
    local step_no="$1"
    local module="$2"
    local description="$3"

    echo "[${step_no}/4] ${description} (${module})..." >> "$LOG_FILE"
    "$PYTHON" -m "$module" >> "$LOG_FILE" 2>&1
    local exit_code=$?
    if [ "$exit_code" -ne 0 ]; then
        fail_step "$module" "$exit_code"
    fi
    echo "✓ [${step_no}/4] ${description}完成" >> "$LOG_FILE"
}

# 在修改数据库或飞书数据前，先检查核心模块是否存在语法错误。
echo "[预检] Python 核心模块语法检查..." >> "$LOG_FILE"
"$PYTHON" -m py_compile \
    "$PROJECT_DIR/jobs/feishu/generate_forecast_comparison.py" \
    "$PROJECT_DIR/jobs/feishu/forecast_sales_improved.py" \
    "$PROJECT_DIR/jobs/feishu/color_system_resolver.py" \
    "$PROJECT_DIR/jobs/feishu/procurement_color_logic.py" \
    "$PROJECT_DIR/jobs/feishu/generate_fabric_forecast.py" \
    "$PROJECT_DIR/jobs/feishu/generate_procurement_report.py" \
    "$PROJECT_DIR/jobs/feishu/generate_fabric_forecast_color_system.py" \
    "$PROJECT_DIR/jobs/feishu/generate_procurement_report_lx_color.py" \
    "$PROJECT_DIR/jobs/feishu/export_procurement_excel_color_system.py" \
    >> "$LOG_FILE" 2>&1
PREFLIGHT_EXIT=$?
if [ "$PREFLIGHT_EXIT" -ne 0 ]; then
    fail_step "Python语法预检" "$PREFLIGHT_EXIT"
fi
echo "✓ Python 核心模块语法检查通过" >> "$LOG_FILE"

run_module "1" "jobs.feishu.write_order_forecast_to_feishu" "同步运营预计下单量"
run_module "2" "jobs.feishu.generate_forecast_comparison" "生成预测对比表"
run_module "3" "jobs.feishu.generate_procurement_report_lx_color" "生成颜色体系采购建议和面料预估"
run_module "4" "jobs.feishu.export_procurement_excel_color_system" "导出颜色体系采购建议 Excel"

END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))

echo "结束时间: $END_TIME，耗时: ${ELAPSED}s" >> "$LOG_FILE"
echo "===================================" >> "$LOG_FILE"
send_feishu_success "$ELAPSED"
