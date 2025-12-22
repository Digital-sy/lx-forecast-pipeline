#!/bin/bash

# ============================================
# 状态检查脚本
# 用途：检查项目运行状态和环境配置
# ============================================

echo "==================================="
echo "项目状态检查"
echo "==================================="

# 检查 Python 环境
echo ""
echo "【Python 环境】"
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "✓ 虚拟环境: 存在"
    echo "  Python 版本: $(python --version)"
    echo "  pip 版本: $(pip --version | awk '{print $2}')"
else
    echo "✗ 虚拟环境不存在"
fi

# 检查配置文件
echo ""
echo "【配置文件】"
if [ -f ".env" ]; then
    echo "✓ .env: 存在"
    # 不显示敏感信息，只显示配置项是否存在
    echo "  配置项检查:"
    grep -E "^[A-Z_]+=" .env | sed 's/=.*/=***/' | head -10
else
    echo "✗ .env: 不存在"
fi

# 检查日志目录
echo ""
echo "【日志目录】"
if [ -d "logs" ]; then
    LOG_SIZE=$(du -sh logs 2>/dev/null | awk '{print $1}')
    LOG_COUNT=$(find logs -name "*.log" 2>/dev/null | wc -l)
    echo "✓ logs: 存在"
    echo "  总大小: $LOG_SIZE"
    echo "  日志文件数: $LOG_COUNT"
    echo ""
    echo "  最近的日志文件:"
    find logs -name "*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -5 | awk '{print "  - " $2}'
else
    echo "✗ logs: 不存在"
fi

# 检查 Git 状态
echo ""
echo "【Git 状态】"
if [ -d ".git" ]; then
    echo "✓ Git 仓库: 存在"
    echo "  当前分支: $(git branch --show-current)"
    echo "  最近提交: $(git log -1 --format='%h - %s (%ar)')"
    
    # 检查是否有未提交的更改
    if [ -n "$(git status --porcelain)" ]; then
        echo "  ⚠ 有未提交的更改"
    else
        echo "  ✓ 工作区干净"
    fi
    
    # 检查是否与远程同步
    LOCAL=$(git rev-parse @ 2>/dev/null)
    REMOTE=$(git rev-parse @{u} 2>/dev/null)
    if [ -z "$REMOTE" ]; then
        echo "  ⚠ 未设置远程分支"
    elif [ "$LOCAL" = "$REMOTE" ]; then
        echo "  ✓ 与远程同步"
    else
        echo "  ⚠ 与远程不同步"
    fi
else
    echo "✗ Git 仓库不存在"
fi

# 检查定时任务
echo ""
echo "【定时任务】"
CRON_COUNT=$(crontab -l 2>/dev/null | grep -c "pythondata" || echo "0")
if [ "$CRON_COUNT" -gt 0 ]; then
    echo "✓ Crontab 任务: $CRON_COUNT 个"
    echo ""
    echo "  任务列表:"
    crontab -l 2>/dev/null | grep "pythondata" | sed 's/^/  /'
else
    echo "✗ 未配置 Crontab 任务"
fi

# 检查依赖包
echo ""
echo "【依赖包】"
if [ -f "requirements.txt" ]; then
    REQUIRED_COUNT=$(grep -c "^[a-zA-Z]" requirements.txt)
    if [ -d "venv" ]; then
        source venv/bin/activate
        INSTALLED_COUNT=$(pip list --format=freeze 2>/dev/null | wc -l)
        echo "✓ requirements.txt: $REQUIRED_COUNT 个依赖"
        echo "  已安装: $INSTALLED_COUNT 个包"
    else
        echo "⚠ requirements.txt: $REQUIRED_COUNT 个依赖"
        echo "  虚拟环境不存在，无法检查安装情况"
    fi
else
    echo "✗ requirements.txt 不存在"
fi

# 测试数据库连接（如果环境已配置）
echo ""
echo "【数据库连接】"
if [ -f ".env" ] && [ -d "venv" ]; then
    source venv/bin/activate
    python -c "
from common.config import settings
from common.database import DatabaseManager
try:
    db = DatabaseManager()
    if db.test_connection():
        print('✓ 数据库连接: 正常')
        print(f'  地址: {settings.DB_HOST}:{settings.DB_PORT}')
        print(f'  数据库: {settings.DB_DATABASE}')
    else:
        print('✗ 数据库连接: 失败')
except Exception as e:
    print(f'✗ 数据库连接测试失败: {e}')
" 2>/dev/null || echo "⚠ 无法测试数据库连接"
else
    echo "⚠ 环境未完全配置，跳过测试"
fi

echo ""
echo "==================================="
echo "检查完成"
echo "==================================="

