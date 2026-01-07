#!/bin/bash
PROJECT_DIR="/opt/apps/pythondata"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON="$VENV_DIR/bin/python"
cd "$PROJECT_DIR" || exit 1
source "$VENV_DIR/bin/activate" || exit 1
echo "==================================="
echo "库存同步任务开始: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="
echo "[1/2] 采集库存明细数据..."
$PYTHON -m jobs.purchase_analysis.fetch_inventory_details
EXIT_CODE_1=$?
[ $EXIT_CODE_1 -eq 0 ] && echo "✓ 采集完成" || echo "✗ 采集失败 (错误码: $EXIT_CODE_1)"
echo "[2/2] 写入飞书多维表..."
$PYTHON -m jobs.feishu.write_inventory_to_feishu
EXIT_CODE_2=$?
[ $EXIT_CODE_2 -eq 0 ] && echo "✓ 写入完成" || echo "✗ 写入失败 (错误码: $EXIT_CODE_2)"
echo "==================================="
echo "库存同步任务结束: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="
[ $EXIT_CODE_1 -eq 0 ] && [ $EXIT_CODE_2 -eq 0 ] && exit 0 || exit 1