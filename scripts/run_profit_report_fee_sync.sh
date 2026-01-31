#!/bin/bash

PROJECT_DIR="/opt/apps/pythondata"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON="$VENV_DIR/bin/python"

# 飞书webhook地址
FEISHU_WEBHOOK_URL="https://open.feishu.cn/open-apis/bot/v2/hook/00640680-6577-4a95-b25a-35c34864ff45"

# 发送飞书消息的函数
send_feishu_message() {
    local message="$1"
    if command -v curl >/dev/null 2>&1; then
        curl -X POST "$FEISHU_WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d "{\"msg_type\":\"text\",\"content\":{\"text\":\"$message\"}}" \
            --max-time 10 \
            --silent --show-error >/dev/null 2>&1
    else
        echo "警告: curl 命令不存在，无法发送飞书消息"
    fi
}

# 处理错误的函数
handle_error() {
    local task_name="$1"
    local exit_code="$2"
    local error_time=$(date '+%Y-%m-%d %H:%M:%S')
    
    echo "❌ $task_name 执行失败 (错误码: $exit_code)"
    
    # 发送飞书通知
    local feishu_message="❌ 利润报表费用单同步任务执行失败

📋 失败任务: $task_name
⏰ 执行时间: $error_time
🔢 错误码: $exit_code

请及时检查日志文件：
$PROJECT_DIR/logs/"

    # 根据任务名称添加对应的日志文件路径
    case "$task_name" in
        "步骤1: 采集利润报表数据")
            feishu_message="${feishu_message}profit_report_msku_daily.log"
            ;;
        "步骤2: 更新计算字段")
            feishu_message="${feishu_message}update_profit_report_calc.log"
            ;;
        "步骤3: 创建费用单")
            feishu_message="${feishu_message}fee_management.log"
            ;;
        *)
            feishu_message="${feishu_message}profit_report_fee_sync.log"
            ;;
    esac
    
    send_feishu_message "$feishu_message"
    exit $exit_code
}

# 发送成功消息
send_success_message() {
    local total_time="$1"
    local feishu_message="✅ 利润报表费用单同步任务执行成功

📊 执行步骤:
  1. ✅ 采集利润报表数据
  2. ✅ 更新计算字段
  3. ✅ 创建费用单

⏱️  总耗时: $total_time

执行时间: $(date '+%Y-%m-%d %H:%M:%S')"
    
    send_feishu_message "$feishu_message"
}

cd "$PROJECT_DIR" || exit 1
source "$VENV_DIR/bin/activate" || exit 1

START_TIME=$(date +%s)

echo "==================================="
echo "利润报表费用单同步任务开始: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="
echo "默认处理范围: 前15天到今天"
echo ""

# 步骤1: 采集利润报表数据
echo "[1/3] 步骤1: 采集利润报表数据..."
$PYTHON -m jobs.Sync_data.fetch_profit_report_msku_daily
EXIT_CODE_1=$?

if [ $EXIT_CODE_1 -ne 0 ]; then
    handle_error "步骤1: 采集利润报表数据" $EXIT_CODE_1
else
    echo "✓ 步骤1完成"
fi

echo ""
echo "[2/3] 步骤2: 更新计算字段..."
$PYTHON -m jobs.Sync_data.update_profit_report_calculated_fields
EXIT_CODE_2=$?

if [ $EXIT_CODE_2 -ne 0 ]; then
    handle_error "步骤2: 更新计算字段" $EXIT_CODE_2
else
    echo "✓ 步骤2完成"
fi

echo ""
echo "[3/3] 步骤3: 创建费用单..."
$PYTHON -m jobs.Sync_data.create_fee_management
EXIT_CODE_3=$?

if [ $EXIT_CODE_3 -ne 0 ]; then
    handle_error "步骤3: 创建费用单" $EXIT_CODE_3
else
    echo "✓ 步骤3完成"
fi

END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))
TOTAL_TIME_STR=$(printf '%02d:%02d:%02d' $((TOTAL_TIME/3600)) $((TOTAL_TIME%3600/60)) $((TOTAL_TIME%60)))

echo ""
echo "==================================="
echo "✅ 所有任务执行成功"
echo "总耗时: $TOTAL_TIME_STR"
echo "完成时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="

# 发送成功通知
send_success_message "$TOTAL_TIME_STR"

exit 0


