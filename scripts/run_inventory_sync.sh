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

if [ $EXIT_CODE_1 -ne 0 ]; then
  echo "✗ 采集失败 (错误码: $EXIT_CODE_1)，终止任务"
  exit 1
else
  echo "✓ 采集完成"
fi

echo "[2/2] 写入飞书多维表..."
$PYTHON -m jobs.feishu.write_inventory_to_feishu
EXIT_CODE_2=$?

if [ $EXIT_CODE_2 -ne 0 ]; then
  echo "✗ 写入飞书失败 (错误码: $EXIT_CODE_2)"
  exit 1
else
  echo "✓ 写入完成"
fi

echo "==================================="
echo "库存同步任务结束: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="
