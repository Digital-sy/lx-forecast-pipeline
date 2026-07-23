#!/bin/bash
# 生成用户需要的两份交付物：
# 1. 有销量但颜色体系待定SKU清单
# 2. 最新版面料-颜色预计下单表（颜色编制表中文名称+颜色体系）
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/opt/apps/pythondata}"
PYTHON="${PYTHON:-$PROJECT_DIR/venv/bin/python}"

cd "$PROJECT_DIR"

"$PYTHON" -m py_compile \
  jobs/feishu/color_mapping_catalog.py \
  jobs/feishu/generate_fabric_forecast_named_colors.py \
  jobs/feishu/generate_procurement_report_named_colors.py \
  jobs/feishu/export_fabric_color_order_forecast.py \
  scripts/export_unresolved_sold_color_system_skus.py

"$PYTHON" scripts/export_unresolved_sold_color_system_skus.py
"$PYTHON" -m jobs.feishu.generate_procurement_report_named_colors
"$PYTHON" -m jobs.feishu.export_fabric_color_order_forecast

echo "完成。输出目录：${PROCUREMENT_EXPORT_DIR:-$PROJECT_DIR/exports}"
