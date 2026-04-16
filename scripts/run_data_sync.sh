#!/bin/bash

# ============================================
# 数据同步任务执行脚本
# 用途：顺序执行Listing、面料参数、面料数据、下单对比、面料预估等任务
# ============================================

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
    local feishu_message="❌ 数据同步任务执行失败

📋 失败任务: $task_name
⏰ 执行时间: $error_time
🔢 错误码: $exit_code

请及时检查日志文件：
/opt/apps/pythondata/logs/cron_data_sync.log"
    
    send_feishu_message "$feishu_message"
    exit $exit_code
}

cd "$PROJECT_DIR" || exit 1
source "$VENV_DIR/bin/activate" || exit 1

echo "==================================="
echo "数据同步任务开始: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="

# 任务1: 采集Listing数据
echo ""
echo "[1/5] 采集亚马逊Listing数据..."
$PYTHON -m jobs.Sync_data.fetch_listing
EXIT_CODE_1=$?

if [ $EXIT_CODE_1 -ne 0 ]; then
    handle_error "采集亚马逊Listing数据" $EXIT_CODE_1
else
    echo "✓ Listing数据采集完成"
fi

# 任务2: 采集面料参数数据
echo ""
echo "[2/5] 采集面料参数数据..."
$PYTHON -m jobs.feishu.fetch_fabric_params
EXIT_CODE_2=$?

if [ $EXIT_CODE_2 -ne 0 ]; then
    handle_error "采集面料参数数据" $EXIT_CODE_2
else
    echo "✓ 面料参数数据采集完成"
fi

# 任务3: 采集飞书面料数据
echo ""
echo "[3/5] 采集飞书面料数据..."
$PYTHON -m jobs.feishu.fetch_feishu_data
EXIT_CODE_3=$?

if [ $EXIT_CODE_3 -ne 0 ]; then
    handle_error "采集飞书面料数据" $EXIT_CODE_3
else
    echo "✓ 飞书面料数据采集完成"
fi

# 任务4: 生成下单对比表
echo ""
echo "[4/5] 生成下单对比表..."
$PYTHON -m jobs.feishu.generate_order_comparison
EXIT_CODE_4=$?

if [ $EXIT_CODE_4 -ne 0 ]; then
    handle_error "生成下单对比表" $EXIT_CODE_4
else
    echo "✓ 下单对比表生成完成"
fi

# 任务5: 生成面料预估表
echo ""
echo "[5/5] 生成面料预估表..."
$PYTHON -m jobs.feishu.generate_fabric_forecast
EXIT_CODE_5=$?

if [ $EXIT_CODE_5 -ne 0 ]; then
    handle_error "生成面料预估表" $EXIT_CODE_5
else
    echo "✓ 面料预估表生成完成"
fi

echo ""
echo "==================================="
echo "数据同步任务结束: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="
echo "✅ 所有任务执行完成！"

