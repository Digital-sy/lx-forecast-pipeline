#!/bin/bash

# ============================================
# 项目更新脚本
# 用途：从 Git 仓库拉取最新代码并更新依赖
# ============================================

set -e  # 遇到错误立即退出

echo "==================================="
echo "开始更新项目"
echo "==================================="

# 检查是否在项目目录
if [ ! -f "requirements.txt" ]; then
    echo "✗ 错误：请在项目根目录执行此脚本"
    exit 1
fi

# 记录当前分支
CURRENT_BRANCH=$(git branch --show-current)
echo "当前分支: $CURRENT_BRANCH"

# 检查是否有未提交的更改
if [ -n "$(git status --porcelain)" ]; then
    echo "⚠ 警告：检测到未提交的更改"
    echo ""
    git status --short
    echo ""
    read -p "是否继续更新？(y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "取消更新"
        exit 1
    fi
fi

# 拉取最新代码
echo ""
echo "[1/4] 拉取最新代码..."
git pull origin $CURRENT_BRANCH
echo "✓ 代码更新完成"

# 激活虚拟环境
echo ""
echo "[2/4] 激活虚拟环境..."
if [ ! -d "venv" ]; then
    echo "✗ 虚拟环境不存在，请先运行 setup_server.sh"
    exit 1
fi
source venv/bin/activate
echo "✓ 虚拟环境已激活"

# 更新依赖
echo ""
echo "[3/4] 更新依赖包..."
pip install -r requirements.txt --upgrade
echo "✓ 依赖更新完成"

# 检查配置文件
echo ""
echo "[4/4] 检查配置文件..."
if [ ! -f ".env" ]; then
    echo "⚠ 警告：.env 文件不存在"
    if [ -f "env.example" ]; then
        read -p "是否从 env.example 创建 .env 文件？(y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            cp env.example .env
            chmod 600 .env
            echo "✓ 已创建 .env 文件，请编辑并填入实际配置"
        fi
    fi
else
    echo "✓ 配置文件存在"
fi

# 完成
echo ""
echo "==================================="
echo "✓ 更新完成！"
echo "==================================="
echo ""
echo "建议进行测试："
echo "  source venv/bin/activate"
echo "  python -m jobs.purchase_order"
echo ""

