#!/bin/bash

# ============================================
# 虚拟环境稳定性检查脚本
# 用途：全面检查虚拟环境配置和运行稳定性
# ============================================

set -e  # 遇到错误立即退出

echo "==================================="
echo "虚拟环境稳定性检查"
echo "==================================="
echo ""

# 检查是否在项目目录
if [ ! -f "requirements.txt" ]; then
    echo "✗ 错误：请在项目根目录执行此脚本"
    exit 1
fi

# 1. 检查虚拟环境是否存在
echo "【1/8】检查虚拟环境..."
if [ ! -d "venv" ]; then
    echo "✗ 虚拟环境不存在"
    echo "  请运行: python3 -m venv venv"
    exit 1
fi
echo "✓ 虚拟环境目录存在"

# 激活虚拟环境
source venv/bin/activate

# 2. 检查 Python 版本
echo ""
echo "【2/8】检查 Python 版本..."
PYTHON_VERSION=$(python --version 2>&1 | awk '{print $2}')
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 8 ]); then
    echo "✗ Python 版本过低: $PYTHON_VERSION (需要 >= 3.8)"
    exit 1
fi
echo "✓ Python 版本: $PYTHON_VERSION"
echo "  Python 路径: $(which python)"

# 3. 检查 pip 版本
echo ""
echo "【3/8】检查 pip 版本..."
PIP_VERSION=$(pip --version | awk '{print $2}')
echo "✓ pip 版本: $PIP_VERSION"

# 4. 检查 requirements.txt 中的依赖
echo ""
echo "【4/8】检查依赖包安装情况..."
if [ ! -f "requirements.txt" ]; then
    echo "✗ requirements.txt 不存在"
    exit 1
fi

REQUIRED_PACKAGES=$(grep -E "^[a-zA-Z]" requirements.txt | grep -v "^#" | awk -F'[>=<,;]' '{print $1}' | tr -d ' ' | sort)
INSTALLED_PACKAGES=$(pip list --format=freeze | awk -F'==' '{print $1}' | tr '[:upper:]' '[:lower:]' | sort)

MISSING_PACKAGES=()
VERSION_MISMATCH=()

for pkg in $REQUIRED_PACKAGES; do
    if [ -z "$pkg" ]; then
        continue
    fi
    
    # 检查是否安装（不区分大小写）
    INSTALLED=$(echo "$INSTALLED_PACKAGES" | grep -i "^${pkg}$" || true)
    
    if [ -z "$INSTALLED" ]; then
        MISSING_PACKAGES+=("$pkg")
    fi
done

if [ ${#MISSING_PACKAGES[@]} -gt 0 ]; then
    echo "✗ 缺少以下依赖包:"
    for pkg in "${MISSING_PACKAGES[@]}"; do
        echo "    - $pkg"
    done
    echo ""
    echo "  请运行: pip install -r requirements.txt"
    exit 1
else
    echo "✓ 所有依赖包已安装"
    INSTALLED_COUNT=$(pip list --format=freeze | wc -l)
    echo "  已安装包总数: $INSTALLED_COUNT"
fi

# 5. 测试导入核心模块
echo ""
echo "【5/8】测试导入核心模块..."
IMPORT_ERRORS=()

# 测试导入公共模块
python -c "
import sys
errors = []

try:
    from common import settings
    print('✓ common.settings')
except Exception as e:
    errors.append(f'common.settings: {e}')

try:
    from common.database import db_cursor
    print('✓ common.database')
except Exception as e:
    errors.append(f'common.database: {e}')

try:
    from common.logger import get_logger
    print('✓ common.logger')
except Exception as e:
    errors.append(f'common.logger: {e}')

try:
    from common.feishu import FeishuClient
    print('✓ common.feishu')
except Exception as e:
    errors.append(f'common.feishu: {e}')

try:
    from utils.data_transform import convert_feishu_record_to_dict
    print('✓ utils.data_transform')
except Exception as e:
    errors.append(f'utils.data_transform: {e}')

try:
    from utils.date_utils import parse_month
    print('✓ utils.date_utils')
except Exception as e:
    errors.append(f'utils.date_utils: {e}')

try:
    from lingxing.openapi import OpenApiBase
    print('✓ lingxing.openapi')
except Exception as e:
    errors.append(f'lingxing.openapi: {e}')

if errors:
    print('✗ 导入错误:', file=sys.stderr)
    for err in errors:
        print(f'  - {err}', file=sys.stderr)
    sys.exit(1)
" 2>&1

if [ $? -ne 0 ]; then
    echo "✗ 模块导入测试失败"
    exit 1
fi

# 6. 测试数据库连接
echo ""
echo "【6/8】测试数据库连接..."
if [ -f ".env" ]; then
    DB_TEST=$(python -c "
from common.database import db_cursor
try:
    with db_cursor() as cursor:
        cursor.execute('SELECT 1')
        result = cursor.fetchone()
        if result:
            print('✓ 数据库连接成功')
        else:
            print('✗ 数据库连接失败: 查询无结果')
            exit(1)
except Exception as e:
    print(f'✗ 数据库连接失败: {e}')
    exit(1)
" 2>&1)
    
    echo "$DB_TEST"
    if echo "$DB_TEST" | grep -q "✗"; then
        echo "⚠ 数据库连接失败，但继续检查其他项"
    fi
else
    echo "⚠ .env 文件不存在，跳过数据库连接测试"
fi

# 7. 测试导入项目模块
echo ""
echo "【7/8】测试导入项目模块..."
PROJECT_IMPORT_ERRORS=()

python -c "
import sys
errors = []

try:
    from jobs.purchase_analysis import main
    print('✓ jobs.purchase_analysis.main')
except Exception as e:
    errors.append(f'jobs.purchase_analysis.main: {e}')

try:
    from jobs.purchase_analysis.fetch_operation import main as fetch_operation_main
    print('✓ jobs.purchase_analysis.fetch_operation')
except Exception as e:
    errors.append(f'jobs.purchase_analysis.fetch_operation: {e}')

try:
    from jobs.purchase_analysis.generate_analysis import main as generate_analysis_main
    print('✓ jobs.purchase_analysis.generate_analysis')
except Exception as e:
    errors.append(f'jobs.purchase_analysis.generate_analysis: {e}')

if errors:
    print('✗ 项目模块导入错误:', file=sys.stderr)
    for err in errors:
        print(f'  - {err}', file=sys.stderr)
    sys.exit(1)
" 2>&1

if [ $? -ne 0 ]; then
    echo "✗ 项目模块导入测试失败"
    exit 1
fi

# 8. 检查关键文件权限
echo ""
echo "【8/8】检查文件权限..."
if [ -d "logs" ]; then
    if [ -w "logs" ]; then
        echo "✓ logs 目录可写"
    else
        echo "✗ logs 目录不可写"
    fi
else
    echo "⚠ logs 目录不存在，将自动创建"
    mkdir -p logs
    echo "✓ 已创建 logs 目录"
fi

# 总结
echo ""
echo "==================================="
echo "检查完成"
echo "==================================="
echo ""
echo "✓ 虚拟环境配置正常"
echo "✓ 所有依赖包已安装"
echo "✓ 核心模块可正常导入"
echo "✓ 项目模块可正常导入"
echo ""
echo "可以安全运行:"
echo "  source venv/bin/activate"
echo "  python -m jobs.purchase_analysis.main"
echo ""


