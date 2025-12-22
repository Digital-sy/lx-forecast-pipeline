#!/bin/bash

# ============================================
# 数据采集任务执行脚本
# 用途：在服务器上定时执行所有数据采集任务
# ============================================

# 设置项目路径（根据实际部署路径修改）
PROJECT_DIR="/opt/apps/pythondata"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON="$VENV_DIR/bin/python"

# 切换到项目目录
cd "$PROJECT_DIR" || exit 1

# 激活虚拟环境
source "$VENV_DIR/bin/activate" || exit 1

# 记录开始时间
echo "==================================="
echo "任务开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="

# 执行采购单采集
echo ""
echo "[1/3] 开始执行: 采购单数据采集..."
$PYTHON -m jobs.purchase_order
if [ $? -eq 0 ]; then
    echo "✓ 采购单数据采集完成"
else
    echo "✗ 采购单数据采集失败 (错误码: $?)"
fi

# 等待一段时间避免API请求过于频繁
sleep 5

# 执行运营下单采集
echo ""
echo "[2/3] 开始执行: 运营下单数据采集..."
$PYTHON -m jobs.operation_order
if [ $? -eq 0 ]; then
    echo "✓ 运营下单数据采集完成"
else
    echo "✗ 运营下单数据采集失败 (错误码: $?)"
fi

# 等待一段时间
sleep 5

# 执行分析表生成
echo ""
echo "[3/3] 开始执行: 分析表生成..."
$PYTHON -m jobs.analysis_table
if [ $? -eq 0 ]; then
    echo "✓ 分析表生成完成"
else
    echo "✗ 分析表生成失败 (错误码: $?)"
fi

# 记录结束时间
echo ""
echo "==================================="
echo "任务结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="
