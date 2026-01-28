#!/bin/bash

PROJECT_DIR="/opt/apps/pythondata"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON="$VENV_DIR/bin/python"

# 飞书webhook地址（与 main.py 保持一致）
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
    local feishu_message="❌ 库存同步任务执行失败

📋 失败任务: $task_name
⏰ 执行时间: $error_time
🔢 错误码: $exit_code

请及时检查日志文件：
/opt/apps/pythondata/logs/cron_inventory_sync.log"
    
    send_feishu_message "$feishu_message"
    exit $exit_code
}

cd "$PROJECT_DIR" || exit 1
source "$VENV_DIR/bin/activate" || exit 1

echo "==================================="
echo "库存同步任务开始: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="

echo "[1/2] 采集库存明细数据..."
$PYTHON -m jobs.purchase_analysis.fetch_inventory_details
EXIT_CODE_1=$?

if [ $EXIT_CODE_1 -ne 0 ]; then
  handle_error "采集库存明细数据" $EXIT_CODE_1
else
  echo "✓ 采集完成"
fi

echo "[2/2] 写入飞书多维表..."
$PYTHON -m jobs.feishu.write_inventory_to_feishu
EXIT_CODE_2=$?

if [ $EXIT_CODE_2 -ne 0 ]; then
  handle_error "写入飞书多维表" $EXIT_CODE_2
else
  echo "✓ 写入完成"
fi

echo "==================================="
echo "库存同步任务结束: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="
