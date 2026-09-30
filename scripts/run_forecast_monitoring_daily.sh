#!/bin/bash
# ============================================
# 销量预测动态监控（影子系统 V2）
#
# 只写 forecast_* 新表，不修改现有生产预测/采购表。
# 1. 当前FBA库存每日快照
# 2. 日维度销量/流量/CVR特征快照
# 3. 当前生产预测永久issue-date快照
# 4. NEW_VISIBLE breakout监控
# 5. 飞书摘要
#
# V2数据源保护：
# - 自动选择最新产品表现源
# - 产品表现数据落后>3天直接失败，不使用陈旧数据
# ============================================

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/opt/apps/pythondata}"
VENV_DIR="${VENV_DIR:-$PROJECT_DIR/venv}"
PYTHON="${PYTHON:-$VENV_DIR/bin/python}"
LOG_DIR="${LOG_DIR:-$PROJECT_DIR/logs}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/cron_forecast_monitoring_daily.log}"
LOCK_FILE="${LOCK_FILE:-/tmp/lx_forecast_monitoring_daily.lock}"

cd "$PROJECT_DIR"
mkdir -p "$LOG_DIR"

if [ ! -x "$PYTHON" ]; then
    echo "Python解释器不存在: $PYTHON" >&2
    exit 1
fi

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') 已有forecast monitoring任务运行，本次跳过" >> "$LOG_FILE"
    exit 0
fi

START_TS=$(date +%s)
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')

echo "============================================================" >> "$LOG_FILE"
echo "开始时间: $START_TIME" >> "$LOG_FILE"

notify_failed() {
    local detail="$1"
    "$PYTHON" "$PROJECT_DIR/scripts/notify_feishu.py" \
      --task "销量预测每日监控" \
      --status failed \
      --detail "$detail；请查看日志 $LOG_FILE" \
      2>/dev/null || true
}

if ! "$PYTHON" -m py_compile \
    "$PROJECT_DIR/jobs/forecast_monitoring/daily_monitor.py" \
    "$PROJECT_DIR/jobs/forecast_monitoring/daily_monitor_v2.py" \
    >> "$LOG_FILE" 2>&1; then
    notify_failed "Python语法预检失败"
    exit 1
fi

echo "✓ Python语法预检通过" >> "$LOG_FILE"

set +e
"$PYTHON" -m jobs.forecast_monitoring.daily_monitor_v2 --notify >> "$LOG_FILE" 2>&1
EXIT_CODE=$?
set -e

if [ "$EXIT_CODE" -ne 0 ]; then
    echo "✗ 每日监控失败，退出码=$EXIT_CODE" >> "$LOG_FILE"
    notify_failed "每日监控失败，退出码=$EXIT_CODE"
    exit "$EXIT_CODE"
fi

END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))
echo "✓ 每日监控完成，耗时 ${ELAPSED}s" >> "$LOG_FILE"
echo "============================================================" >> "$LOG_FILE"
