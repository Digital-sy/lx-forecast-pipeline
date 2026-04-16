#!/bin/bash

# ============================================
# 项目更新脚本（强制同步版）
# 用途：从 Git 仓库拉取最新代码并更新依赖
# 策略：以远程仓库为准，直接覆盖本地修改
# ============================================

set -e  # 遇到错误立即退出

echo "==================================="
echo "开始更新项目（强制同步模式）"
echo "==================================="

# 检查是否在项目目录
if [ ! -f "requirements.txt" ]; then
    echo "✗ 错误：请在项目根目录执行此脚本"
    exit 1
fi

# 记录当前分支
CURRENT_BRANCH=$(git branch --show-current 2>/dev/null || echo "main")
echo "当前分支: $CURRENT_BRANCH"

# 检查是否有未提交的更改
echo ""
echo "[1/5] 检查并清理本地修改..."
if [ -n "$(git status --porcelain 2>/dev/null)" ]; then
    echo "⚠ 检测到本地修改，将自动丢弃并使用远程版本"
    git status --short | sed 's/^/  - /'
    echo ""
    echo "正在丢弃本地修改..."
    git checkout -- . 2>/dev/null || true
    git clean -fd 2>/dev/null || true
    echo "✓ 本地修改已清理"
else
    echo "✓ 工作区干净，无本地修改"
fi

# 拉取最新代码（强制使用远程版本）
echo ""
echo "[2/5] 拉取最新代码..."
# 先获取远程更新
git fetch origin $CURRENT_BRANCH

# 重置到远程版本（强制覆盖）
LOCAL_COMMIT=$(git rev-parse HEAD 2>/dev/null)
REMOTE_COMMIT=$(git rev-parse origin/$CURRENT_BRANCH 2>/dev/null)

if [ "$LOCAL_COMMIT" != "$REMOTE_COMMIT" ]; then
    echo "  本地提交: ${LOCAL_COMMIT:0:8}"
    echo "  远程提交: ${REMOTE_COMMIT:0:8}"
    echo "  正在重置到远程版本..."
    git reset --hard origin/$CURRENT_BRANCH
    echo "✓ 已强制同步到远程版本"
else
    echo "✓ 代码已是最新版本"
fi

# 激活虚拟环境
echo ""
echo "[3/5] 激活虚拟环境..."
if [ ! -d "venv" ]; then
    echo "✗ 虚拟环境不存在，请先运行 setup_server.sh"
    exit 1
fi
source venv/bin/activate
echo "✓ 虚拟环境已激活"

# 更新依赖
echo ""
echo "[4/5] 更新依赖包..."
pip install -r requirements.txt --upgrade --quiet
echo "✓ 依赖更新完成"

# 设置脚本执行权限
echo ""
echo "[5/5] 设置脚本执行权限..."
SCRIPT_DIR="scripts"
if [ -d "$SCRIPT_DIR" ]; then
    find "$SCRIPT_DIR" -name "*.sh" -type f -exec chmod +x {} \; 2>/dev/null
    SCRIPT_COUNT=$(find "$SCRIPT_DIR" -name "*.sh" -type f | wc -l)
    echo "✓ 已设置 $SCRIPT_COUNT 个脚本的执行权限"
else
    echo "⚠ 脚本目录不存在"
fi

# 检查配置文件
echo ""
echo "检查配置文件..."
if [ ! -f ".env" ]; then
    echo "⚠ 警告：.env 文件不存在"
    if [ -f "env.example" ]; then
        echo "  提示：可以从 env.example 创建 .env 文件"
    fi
else
    echo "✓ 配置文件存在"
fi

# 显示更新摘要
echo ""
echo "==================================="
echo "✓ 更新完成！"
echo "==================================="
echo ""
echo "更新摘要："
echo "  分支: $CURRENT_BRANCH"
echo "  最新提交: $(git log -1 --format='%h - %s (%ar)' 2>/dev/null || echo '未知')"
echo "  提交哈希: $(git rev-parse --short HEAD 2>/dev/null || echo '未知')"
echo ""
echo "注意：所有本地修改已被远程版本覆盖"
echo ""


