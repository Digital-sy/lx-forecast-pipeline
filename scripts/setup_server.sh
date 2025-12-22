#!/bin/bash

# ============================================
# 服务器初始化脚本
# 用途：在新服务器上快速部署项目
# ============================================

set -e  # 遇到错误立即退出

echo "==================================="
echo "开始部署 pythondata 项目"
echo "==================================="

# 检查 Python 版本
echo ""
echo "[1/8] 检查 Python 环境..."
if ! command -v python3 &> /dev/null; then
    echo "✗ Python3 未安装，请先安装 Python 3.8+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python 版本: $PYTHON_VERSION"

# 检查 Git
echo ""
echo "[2/8] 检查 Git..."
if ! command -v git &> /dev/null; then
    echo "✗ Git 未安装，请先安装 Git"
    exit 1
fi
echo "✓ Git 已安装"

# 创建虚拟环境
echo ""
echo "[3/8] 创建 Python 虚拟环境..."
if [ -d "venv" ]; then
    echo "⚠ 虚拟环境已存在，跳过创建"
else
    python3 -m venv venv
    echo "✓ 虚拟环境创建完成"
fi

# 激活虚拟环境
echo ""
echo "[4/8] 激活虚拟环境..."
source venv/bin/activate
echo "✓ 虚拟环境已激活"

# 升级 pip
echo ""
echo "[5/8] 升级 pip..."
pip install --upgrade pip -q
echo "✓ pip 升级完成"

# 安装依赖
echo ""
echo "[6/8] 安装项目依赖..."
pip install -r requirements.txt
echo "✓ 依赖安装完成"

# 配置环境变量
echo ""
echo "[7/8] 配置环境变量..."
if [ -f ".env" ]; then
    echo "⚠ .env 文件已存在，跳过配置"
else
    if [ -f "env.example" ]; then
        cp env.example .env
        echo "✓ 已从 env.example 创建 .env 文件"
        echo "⚠ 请编辑 .env 文件并填入实际配置："
        echo "   vim .env"
    else
        echo "✗ env.example 文件不存在"
    fi
fi

# 设置权限
echo ""
echo "[8/8] 设置脚本权限..."
chmod +x scripts/*.sh
chmod 600 .env 2>/dev/null || true
echo "✓ 权限设置完成"

# 完成
echo ""
echo "==================================="
echo "✓ 部署完成！"
echo "==================================="
echo ""
echo "接下来的步骤："
echo "1. 编辑 .env 文件填入实际配置"
echo "   vim .env"
echo ""
echo "2. 测试运行各个任务"
echo "   source venv/bin/activate"
echo "   python -m jobs.purchase_order"
echo "   python -m jobs.operation_order"
echo "   python -m jobs.analysis_table"
echo ""
echo "3. 配置定时任务"
echo "   crontab -e"
echo "   添加内容参考 DEPLOY.md"
echo ""

