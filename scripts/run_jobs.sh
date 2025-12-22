#!/bin/bash

# ============================================
# 数据采集任务执行脚本
# 用途：在服务器上定时执行所有项目
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

# 执行采购下单分析项目
echo ""
echo "[1/1] 开始执行: 采购下单分析项目..."
$PYTHON -m jobs.purchase_analysis.main
if [ $? -eq 0 ]; then
    echo "✓ 采购下单分析项目完成"
else
    echo "✗ 采购下单分析项目失败 (错误码: $?)"
fi

# 如果有更多项目，在这里添加
# echo ""
# echo "[2/2] 开始执行: 其他项目..."
# $PYTHON -m jobs.other_project.main

# 记录结束时间
echo ""
echo "==================================="
echo "任务结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="
