#!/bin/bash
# ============================================
# 采购建议核心流水线
# 1. 飞书运营预计下单量 → MySQL
# 2. 系统预测 vs 运营预计 → 预测对比表 / 预测对比表_SKU
# 3. 颜色体系建议下单量 + 面料详细预估 → MySQL / 飞书
#    A2023/B2024 仅使用 SKU 自身明确标签，不由飞书颜色反推
# 4. 导出采购建议 Excel
# 5. 最终面料预估：A/B 使用同一批源数据快照；
#    方案B导出业务21列并覆盖飞书，方案A每日快照保留MySQL
#    最终颜色优先使用 SPU 人工映射，再走确定性规则/历史人工映射
#
# 任一步骤失败都会停止后续任务并发送飞书告警。
# ============================================

set -u

PROJECT_DIR="${PROJECT_DIR:-/opt/apps/pythondata}"
VENV_DIR="${VENV_DIR:-$PROJECT_DIR/venv}"
PYTHON="${PYTHON:-$VENV_DIR/bin/python}"
LOG_DIR="${LOG_DIR:-$PROJECT_DIR/logs}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/cron_procurement_pipeline.log}"
SHARED_CONFIG_DIR="${SHARED_CONFIG_DIR:-/opt/apps/pythondata/shared_config}"
HISTORICAL_MANUAL_MAPPING="${FABRIC_COLOR_MANUAL_MAPPING_PATH:-$SHARED_CONFIG_DIR/fabric_color_manual_mapping.csv}"
SPU_MANUAL_MAPPING="${FABRIC_COLOR_SPU_MANUAL_MAPPING_PATH:-$SHARED_CONFIG_DIR/fabric_color_manual_mapping_spu.csv}"
FABRIC_FORECAST_FEISHU_TABLE_NAME="${FABRIC_FORECAST_FEISHU_TABLE_NAME:-面料预估明细}"
FABRIC_FORECAST_SCHEME_A_HISTORY_TABLE="${FABRIC_FORECAST_SCHEME_A_HISTORY_TABLE:-面料预估方案A历史}"

# 第3步仍写建议下单量/面料汇总到飞书，但不再用旧版逻辑覆盖“面料预估明细”。
# 最终业务21列表由第5步唯一负责写入该表，避免同一轮任务重复改表结构。
export SKIP_LEGACY_FABRIC_DETAIL_FEISHU="${SKIP_LEGACY_FABRIC_DETAIL_FEISHU:-1}"

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
        --detail "五个业务步骤全部完成：A/B基于同一数据快照；方案B已更新飞书业务21列，方案A当日快照已保留MySQL" \
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
    shift 3

    echo "[${step_no}/5] ${description} (${module})..." >> "$LOG_FILE"
    "$PYTHON" -m "$module" "$@" >> "$LOG_FILE" 2>&1
    local exit_code=$?
    if [ "$exit_code" -ne 0 ]; then
        fail_step "$module" "$exit_code"
    fi
    echo "✓ [${step_no}/5] ${description}完成" >> "$LOG_FILE"
}

# 在修改数据库或飞书数据前，先检查核心模块是否存在语法错误。
echo "[预检] Python 核心模块语法检查..." >> "$LOG_FILE"
"$PYTHON" -m py_compile \
    "$PROJECT_DIR/jobs/feishu/generate_forecast_comparison.py" \
    "$PROJECT_DIR/jobs/feishu/forecast_sales_improved.py" \
    "$PROJECT_DIR/jobs/feishu/color_system_resolver.py" \
    "$PROJECT_DIR/jobs/feishu/color_mapping_catalog.py" \
    "$PROJECT_DIR/jobs/feishu/procurement_color_logic.py" \
    "$PROJECT_DIR/jobs/feishu/fabric_merge_rule_loader.py" \
    "$PROJECT_DIR/jobs/feishu/generate_fabric_forecast.py" \
    "$PROJECT_DIR/jobs/feishu/generate_procurement_report.py" \
    "$PROJECT_DIR/jobs/feishu/generate_fabric_forecast_color_system.py" \
    "$PROJECT_DIR/jobs/feishu/generate_fabric_forecast_named_colors.py" \
    "$PROJECT_DIR/jobs/feishu/generate_procurement_report_lx_color.py" \
    "$PROJECT_DIR/jobs/feishu/generate_procurement_report_named_colors.py" \
    "$PROJECT_DIR/jobs/feishu/export_procurement_excel_color_system.py" \
    "$PROJECT_DIR/jobs/feishu/fabric_color_stocking.py" \
    "$PROJECT_DIR/jobs/feishu/fabric_color_stocking_spu.py" \
    "$PROJECT_DIR/jobs/feishu/export_fabric_color_order_forecast_final.py" \
    "$PROJECT_DIR/jobs/feishu/export_fabric_color_order_forecast_business.py" \
    "$PROJECT_DIR/jobs/feishu/export_fabric_color_order_forecast_business_ab.py" \
    >> "$LOG_FILE" 2>&1
PREFLIGHT_EXIT=$?
if [ "$PREFLIGHT_EXIT" -ne 0 ]; then
    fail_step "Python语法预检" "$PREFLIGHT_EXIT"
fi
echo "✓ Python 核心模块语法检查通过" >> "$LOG_FILE"

run_module "1" "jobs.feishu.write_order_forecast_to_feishu" "同步运营预计下单量"
run_module "2" "jobs.feishu.generate_forecast_comparison" "生成预测对比表"
run_module "3" "jobs.feishu.generate_procurement_report_named_colors" "生成中文颜色体系采购建议和面料预估"
run_module "4" "jobs.feishu.export_procurement_excel_color_system" "导出颜色体系采购建议 Excel"
run_module "5" "jobs.feishu.export_fabric_color_order_forecast_business_ab" "同源快照计算A/B：方案B写飞书，方案A留MySQL" \
    --manual-mapping "$HISTORICAL_MANUAL_MAPPING" \
    --spu-manual-mapping "$SPU_MANUAL_MAPPING" \
    --write-feishu \
    --feishu-table-name "$FABRIC_FORECAST_FEISHU_TABLE_NAME" \
    --scheme-a-history-table "$FABRIC_FORECAST_SCHEME_A_HISTORY_TABLE"

END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))

echo "结束时间: $END_TIME，耗时: ${ELAPSED}s" >> "$LOG_FILE"
echo "===================================" >> "$LOG_FILE"
send_feishu_success "$ELAPSED"
