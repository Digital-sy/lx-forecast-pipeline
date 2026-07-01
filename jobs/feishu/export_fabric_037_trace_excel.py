#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
导出 037超绒 面料使用情况与预估用量溯源 Excel。

输出内容：
1. 面料预估结果：来自 `面料预估表`
2. SPU用料关系：来自 `面料核价表`
3. SKU用量溯源：运营预计下单表 + 预测对比表_SKU + 面料核价表
4. 库存明细：定制面料参数 + 仓库库存明细
5. 颜色归并：面料颜色归并对照
6. 定制面料参数
7. 检查项：关键异常提示

运行示例：
    python -m jobs.feishu.export_fabric_037_trace_excel
    python -m jobs.feishu.export_fabric_037_trace_excel --fabric-keyword 037 --output exports/037超绒溯源.xlsx
"""

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor

logger = get_logger('fabric_037_trace_export')

try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.table import Table, TableStyleInfo
except ImportError as e:
    raise SystemExit(
        "缺少 openpyxl 依赖，请先执行：\n"
        "  cd /opt/apps/pythondata\n"
        "  ./venv/bin/pip install openpyxl\n"
        "然后重新运行本脚本。"
    ) from e


DEFAULT_KEYWORDS = ['037', '超绒']


def _like_conditions(column_names: List[str], keywords: List[str]) -> Tuple[str, List[str]]:
    parts = []
    params: List[str] = []
    for col in column_names:
        for kw in keywords:
            parts.append(f"`{col}` LIKE %s")
            params.append(f"%{kw}%")
    return '(' + ' OR '.join(parts) + ')', params


def _fetch_all(sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    with db_cursor(dictionary=True) as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())


def _table_exists(table_name: str) -> bool:
    rows = _fetch_all(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
        """,
        (table_name,),
    )
    return bool(rows and rows[0].get('cnt'))


def _normalize_date(value: Any) -> str:
    if value is None:
        return ''
    if hasattr(value, 'strftime'):
        return value.strftime('%Y-%m-%d')
    return str(value)[:10]


def _extract_spu_sql(alias: str = 'k') -> str:
    # 与现有脚本逻辑保持接近：按 SKU 第一个 '-' 前内容识别 SPU。
    return f"SUBSTRING_INDEX({alias}.SKU, '-', 1)"


def read_fabric_params(keywords: List[str]) -> List[Dict[str, Any]]:
    if not _table_exists('定制面料参数'):
        return []
    where, params = _like_conditions(['面料', '面料编号'], keywords)
    return _fetch_all(
        f"""
        SELECT 面料, 面料编号, 米数每条, 公斤数每条
        FROM `定制面料参数`
        WHERE {where}
        ORDER BY 面料, 面料编号
        """,
        tuple(params),
    )


def read_forecast_result(keywords: List[str]) -> List[Dict[str, Any]]:
    if not _table_exists('面料预估表'):
        return []
    where, params = _like_conditions(['面料', '面料编号', '面料颜色编号'], keywords)
    rows = _fetch_all(
        f"""
        SELECT
            统计类型,
            月份,
            面料,
            面料编号,
            颜色缩写,
            颜色,
            面料颜色编号,
            统计日期,
            运营预计下单量,
            系统预估下单量,
            `预计用量/米`,
            `系统预估用量/米`,
            米数每条,
            `预计用量/条`,
            `系统预估用量/条`,
            `库存量/条`,
            `库存量/米`,
            `待到货量/条`,
            `待到货量/米`,
            `预计总量/条`,
            `预计总量/米`,
            GREATEST(
                0,
                IFNULL(`系统预估用量/条`, 0)
                - IFNULL(`库存量/条`, 0)
                - IFNULL(`待到货量/条`, 0)
            ) AS 系统预计采购条数,
            用量信息缺失SPU,
            更新时间
        FROM `面料预估表`
        WHERE {where}
        ORDER BY 统计类型, 统计日期, 面料, 颜色缩写
        """,
        tuple(params),
    )
    for r in rows:
        r['统计日期'] = _normalize_date(r.get('统计日期'))
        r['更新时间'] = _normalize_date(r.get('更新时间'))
    return rows


def read_spu_fabric_usage(keywords: List[str]) -> List[Dict[str, Any]]:
    if not _table_exists('面料核价表'):
        return []
    where, params = _like_conditions(['面料'], keywords)
    return _fetch_all(
        f"""
        SELECT
            h.SPU,
            h.面料,
            h.单件用量,
            h.单件损耗,
            CASE
                WHEN IFNULL(h.单件用量, 0) = (
                    SELECT MAX(IFNULL(h2.单件用量, 0))
                    FROM `面料核价表` h2
                    WHERE h2.SPU = h.SPU
                ) THEN '主面料'
                ELSE '非主面料'
            END AS 主面料判定
        FROM `面料核价表` h
        WHERE {where}
        ORDER BY h.SPU, h.单件用量 DESC
        """,
        tuple(params),
    )


def read_sku_trace(keywords: List[str]) -> List[Dict[str, Any]]:
    if not _table_exists('面料核价表'):
        return []

    has_op = _table_exists('运营预计下单表')
    has_sys = _table_exists('预测对比表_SKU')
    if not has_op and not has_sys:
        return []

    keys_parts = []
    if has_op:
        keys_parts.append("""
            SELECT SKU, 统计日期
            FROM `运营预计下单表`
            WHERE SKU IS NOT NULL AND SKU != '' AND 统计日期 IS NOT NULL AND 预计下单量 > 0
        """)
    if has_sys:
        keys_parts.append("""
            SELECT SKU, 统计日期
            FROM `预测对比表_SKU`
            WHERE SKU IS NOT NULL AND SKU != '' AND 统计日期 IS NOT NULL AND 系统预测销量 > 0
        """)
    keys_union = " UNION ".join(keys_parts)

    op_cte = """
        op AS (
            SELECT SKU, 统计日期, SUM(预计下单量) AS 运营预计下单量
            FROM `运营预计下单表`
            GROUP BY SKU, 统计日期
        )
    """ if has_op else "op AS (SELECT NULL AS SKU, NULL AS 统计日期, 0 AS 运营预计下单量)"

    sys_cte = """
        sysf AS (
            SELECT SKU, 统计日期, SUM(系统预测销量) AS 系统预估下单量
            FROM `预测对比表_SKU`
            GROUP BY SKU, 统计日期
        )
    """ if has_sys else "sysf AS (SELECT NULL AS SKU, NULL AS 统计日期, 0 AS 系统预估下单量)"

    where, params = _like_conditions(['面料'], keywords)
    sql = f"""
        WITH keys_union AS (
            {keys_union}
        ),
        {op_cte},
        {sys_cte}
        SELECT
            k.SKU,
            {_extract_spu_sql('k')} AS SPU,
            k.统计日期,
            h.面料,
            h.单件用量,
            h.单件损耗,
            CASE
                WHEN IFNULL(h.单件用量, 0) = (
                    SELECT MAX(IFNULL(h2.单件用量, 0))
                    FROM `面料核价表` h2
                    WHERE h2.SPU = h.SPU
                ) THEN '主面料'
                ELSE '非主面料'
            END AS 主面料判定,
            IFNULL(op.运营预计下单量, 0) AS 运营预计下单量,
            IFNULL(sysf.系统预估下单量, 0) AS 系统预估下单量,
            ROUND(
                IFNULL(op.运营预计下单量, 0)
                * IFNULL(h.单件用量, 0)
                * CASE WHEN IFNULL(h.单件损耗, 0) = 0 THEN 1 ELSE h.单件损耗 END,
                2
            ) AS 运营预计用量_米,
            ROUND(
                IFNULL(sysf.系统预估下单量, 0)
                * IFNULL(h.单件用量, 0)
                * CASE WHEN IFNULL(h.单件损耗, 0) = 0 THEN 1 ELSE h.单件损耗 END,
                2
            ) AS 系统预估用量_米
        FROM keys_union k
        JOIN `面料核价表` h
            ON h.SPU = {_extract_spu_sql('k')}
        LEFT JOIN op
            ON op.SKU = k.SKU AND op.统计日期 = k.统计日期
        LEFT JOIN sysf
            ON sysf.SKU = k.SKU AND sysf.统计日期 = k.统计日期
        WHERE {where}
        ORDER BY k.统计日期, h.SPU, k.SKU
    """
    rows = _fetch_all(sql, tuple(params))
    for r in rows:
        r['统计日期'] = _normalize_date(r.get('统计日期'))
    return rows


def read_inventory(keywords: List[str]) -> List[Dict[str, Any]]:
    if not _table_exists('定制面料参数') or not _table_exists('仓库库存明细'):
        return []
    where, params = _like_conditions(['面料', '面料编号'], keywords)
    return _fetch_all(
        f"""
        SELECT
            p.面料,
            p.面料编号,
            w.SKU AS 仓库SKU,
            SUM(IFNULL(w.可用量, 0)) AS 可用量,
            SUM(IFNULL(w.待到货量, 0)) AS 待到货量
        FROM `定制面料参数` p
        JOIN `仓库库存明细` w
            ON w.SKU LIKE CONCAT(p.面料编号, '-%')
        WHERE {where}
        GROUP BY p.面料, p.面料编号, w.SKU
        ORDER BY w.SKU
        """,
        tuple(params),
    )


def read_color_merge(keywords: List[str]) -> List[Dict[str, Any]]:
    if not _table_exists('面料颜色归并对照'):
        return []
    where, params = _like_conditions(['面料编号'], keywords)
    return _fetch_all(
        f"""
        SELECT 面料编号, 原始颜色缩写, 归并颜色缩写, 是否启用
        FROM `面料颜色归并对照`
        WHERE {where}
        ORDER BY 面料编号, 原始颜色缩写
        """,
        tuple(params),
    )


def build_check_items(
    params_rows: List[Dict[str, Any]],
    result_rows: List[Dict[str, Any]],
    spu_rows: List[Dict[str, Any]],
    sku_rows: List[Dict[str, Any]],
    inventory_rows: List[Dict[str, Any]],
    color_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    checks = []

    def add(item: str, status: str, detail: str):
        checks.append({'检查项': item, '状态': status, '说明': detail})

    add('定制面料参数', '正常' if params_rows else '异常', f'命中 {len(params_rows)} 条')
    add('面料核价表/SPU用料关系', '正常' if spu_rows else '异常', f'命中 {len(spu_rows)} 个 SPU-面料关系')
    add('SKU用量溯源', '正常' if sku_rows else '异常', f'命中 {len(sku_rows)} 条 SKU+月份 来源明细')
    add('面料预估表最终结果', '正常' if result_rows else '异常', f'命中 {len(result_rows)} 条最终预估结果')
    add('库存匹配', '正常' if inventory_rows else '提醒', f'命中 {len(inventory_rows)} 条库存明细；若为0，检查面料编号是否能匹配仓库库存SKU前缀')
    add('颜色归并', '正常' if color_rows else '提醒', f'命中 {len(color_rows)} 条颜色归并；没有不一定异常')

    zero_usage_spu = [r for r in spu_rows if float(r.get('单件用量') or 0) <= 0]
    add('单件用量为0', '异常' if zero_usage_spu else '正常', f'{len(zero_usage_spu)} 条 SPU 用量为0')

    zero_roll = [r for r in params_rows if float(r.get('米数每条') or 0) <= 0]
    add('米数每条为0', '异常' if zero_roll else '正常', f'{len(zero_roll)} 条定制面料参数 米数每条<=0')

    missing_usage = [r for r in result_rows if r.get('用量信息缺失SPU')]
    add('用量信息缺失SPU', '异常' if missing_usage else '正常', f'{len(missing_usage)} 条结果存在用量信息缺失SPU')

    return checks


def _safe_sheet_name(name: str) -> str:
    invalid = r'[]:*?/\\'
    for ch in invalid:
        name = name.replace(ch, '_')
    return name[:31]


def write_sheet(wb: Workbook, title: str, rows: List[Dict[str, Any]], freeze: str = 'A2'):
    ws = wb.create_sheet(_safe_sheet_name(title))
    if not rows:
        ws.append(['提示'])
        ws.append(['无数据'])
        ws.freeze_panes = freeze
        return ws

    headers = list(rows[0].keys())
    ws.append(headers)
    for row in rows:
        ws.append([row.get(h) for h in headers])

    header_fill = PatternFill('solid', fgColor='1F4E78')
    header_font = Font(color='FFFFFF', bold=True)
    thin = Side(style='thin', color='D9E2F3')
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = border

    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.border = border
            cell.alignment = Alignment(vertical='center')

    ws.freeze_panes = freeze
    ws.auto_filter.ref = ws.dimensions

    # 表格样式
    end_row = ws.max_row
    end_col = ws.max_column
    if end_row >= 2 and end_col >= 1:
        ref = f"A1:{get_column_letter(end_col)}{end_row}"
        table_name = re.sub(r'[^A-Za-z0-9_]', '_', f"tbl_{title}")[:250]
        if not re.match(r'^[A-Za-z_]', table_name):
            table_name = f'T_{table_name}'
        table = Table(displayName=table_name, ref=ref)
        table.tableStyleInfo = TableStyleInfo(
            name='TableStyleMedium2',
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False,
        )
        try:
            ws.add_table(table)
        except Exception:
            pass

    # 自适应列宽，避免过宽
    for idx, col_cells in enumerate(ws.columns, start=1):
        max_len = 8
        for cell in col_cells:
            val = '' if cell.value is None else str(cell.value)
            max_len = max(max_len, len(val))
        ws.column_dimensions[get_column_letter(idx)].width = min(max(max_len + 2, 10), 38)

    return ws


def write_summary(wb: Workbook, summary: Dict[str, Any], checks: List[Dict[str, Any]]):
    ws = wb.active
    ws.title = '汇总说明'
    ws.append(['037超绒面料使用情况与预估用量溯源'])
    ws.append(['生成时间', summary['生成时间']])
    ws.append(['关键词', ', '.join(summary['关键词'])])
    ws.append(['输出说明', '本文件用于核查037超绒的最终预估、SPU用料、SKU来源、库存和颜色归并。'])
    ws.append([])
    ws.append(['数据量汇总'])
    for key, value in summary['数据量汇总'].items():
        ws.append([key, value])
    ws.append([])
    ws.append(['检查项', '状态', '说明'])
    for r in checks:
        ws.append([r['检查项'], r['状态'], r['说明']])

    ws.merge_cells('A1:C1')
    ws['A1'].font = Font(bold=True, size=16, color='FFFFFF')
    ws['A1'].fill = PatternFill('solid', fgColor='1F4E78')
    ws['A1'].alignment = Alignment(horizontal='center')

    for row in range(2, ws.max_row + 1):
        for col in range(1, 4):
            ws.cell(row=row, column=col).alignment = Alignment(vertical='center')
    for cell in ws[6]:
        cell.font = Font(bold=True)
    # 找检查项表头
    for row in range(1, ws.max_row + 1):
        if ws.cell(row=row, column=1).value == '检查项':
            for cell in ws[row]:
                cell.fill = PatternFill('solid', fgColor='D9EAF7')
                cell.font = Font(bold=True)
            break
    ws.column_dimensions['A'].width = 24
    ws.column_dimensions['B'].width = 12
    ws.column_dimensions['C'].width = 80
    ws.freeze_panes = 'A2'


def export_excel(output_path: str, keywords: List[str]):
    logger.info('开始读取037超绒相关数据...')
    params_rows = read_fabric_params(keywords)
    result_rows = read_forecast_result(keywords)
    spu_rows = read_spu_fabric_usage(keywords)
    sku_rows = read_sku_trace(keywords)
    inventory_rows = read_inventory(keywords)
    color_rows = read_color_merge(keywords)

    checks = build_check_items(params_rows, result_rows, spu_rows, sku_rows, inventory_rows, color_rows)
    summary = {
        '生成时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        '关键词': keywords,
        '数据量汇总': {
            '面料预估结果': len(result_rows),
            'SPU用料关系': len(spu_rows),
            'SKU用量溯源': len(sku_rows),
            '库存明细': len(inventory_rows),
            '颜色归并': len(color_rows),
            '定制面料参数': len(params_rows),
        }
    }

    wb = Workbook()
    write_summary(wb, summary, checks)
    write_sheet(wb, '01_面料预估结果', result_rows)
    write_sheet(wb, '02_SPU用料关系', spu_rows)
    write_sheet(wb, '03_SKU用量溯源', sku_rows)
    write_sheet(wb, '04_库存明细', inventory_rows)
    write_sheet(wb, '05_颜色归并', color_rows)
    write_sheet(wb, '06_定制面料参数', params_rows)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output)
    logger.info(f'✓ Excel 已生成: {output}')
    logger.info('数据量汇总: ' + ', '.join(f'{k}={v}' for k, v in summary['数据量汇总'].items()))


def parse_args():
    parser = argparse.ArgumentParser(description='导出037超绒面料使用情况与预估用量溯源Excel')
    parser.add_argument(
        '--fabric-keyword',
        action='append',
        default=None,
        help='面料匹配关键词，可重复传入。默认：037、超绒',
    )
    parser.add_argument(
        '--output',
        default=None,
        help='输出Excel路径，默认 exports/037超绒面料溯源_YYYYMMDD_HHMMSS.xlsx',
    )
    return parser.parse_args()


def main():
    args = parse_args()
    keywords = args.fabric_keyword or DEFAULT_KEYWORDS
    output_path = args.output or f"exports/037超绒面料溯源_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    export_excel(output_path, keywords)


if __name__ == '__main__':
    main()
