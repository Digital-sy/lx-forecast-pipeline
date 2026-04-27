#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
fetch_feishu_data.py 的三处改动，新增同步 核价类型 和 适用部位 字段。

改动点：
  1. extract_fabric_records  —— 多提取 核价类型（记录级）、适用部位（面料级）
  2. create_fabric_table_if_not_exists —— 用 ALTER TABLE 兼容式加列（表已存在时不会报错）
  3. insert_fabric_data_batch —— INSERT / ON DUPLICATE KEY UPDATE 加入两个新字段

直接用下面三个函数替换原文件中对应的同名函数即可，其余代码不变。
"""

# ────────────────────────────────────────────────────────────────────────────
# 1. extract_fabric_records
# ────────────────────────────────────────────────────────────────────────────

def extract_fabric_records(record, field_map):
    """
    从一条飞书记录中提取面料信息，展开成多条记录。
    新增：核价类型（记录级，所有面料共享）、适用部位（面料级）。
    """
    fields = record.get('fields', {})

    # 记录级基础字段
    spu = extract_text_value(fields.get('款号', ''))
    season = extract_text_value(fields.get('季节', ''))

    # 核价类型：记录级，所有面料共用同一个值
    pricing_type = extract_text_value(fields.get('核价类型', ''))

    create_time = fields.get('创建时间', 0)
    if create_time and isinstance(create_time, (int, float)):
        from datetime import datetime
        create_time_str = datetime.fromtimestamp(create_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
    else:
        create_time_str = str(create_time)

    fabric_records = []

    for i in range(1, 4):  # 面料1 ~ 面料3
        fabric_name = extract_text_value(fields.get(f'面料{i}', ''))
        if not fabric_name:
            continue

        usage     = extract_numeric_value(fields.get(f'单件用量/M-面料{i}', 0))
        loss      = extract_numeric_value(fields.get(f'单件损耗/M-面料{i}', 0))
        price     = extract_numeric_value(fields.get(f'单件价格-面料{i}', 0))
        unit_price = extract_numeric_value(fields.get(f'面料{i}单价', 0))

        # 适用部位：面料级，每个面料独立
        applicable_part = extract_text_value(fields.get(f'适用部位-面料{i}', ''))

        fabric_records.append({
            'SPU':       spu,
            '季节':      season,
            '面料':      fabric_name,
            '单件用量':  usage,
            '单件损耗':  loss,
            '单件价格':  price,
            '面料单价':  unit_price,
            '核价类型':  pricing_type,      # 新增
            '适用部位':  applicable_part,   # 新增
            '创建时间':  create_time_str,
            '创建时间戳': create_time,      # 供 filter_latest_by_spu 排序用，不入库
        })

    return fabric_records


# ────────────────────────────────────────────────────────────────────────────
# 2. create_fabric_table_if_not_exists
# ────────────────────────────────────────────────────────────────────────────

def create_fabric_table_if_not_exists():
    """
    创建面料核价表（如果不存在）；
    若表已存在则用 ALTER TABLE 兼容式追加 核价类型、适用部位 两列。
    """
    from common.database import db_cursor
    import logging
    logger = logging.getLogger('feishu_fetch_data')
    logger.info("正在检查/创建面料核价表...")

    try:
        with db_cursor(dictionary=False) as cursor:
            # 建表（含新字段）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `面料核价表` (
                    `id`           INT AUTO_INCREMENT PRIMARY KEY,
                    `SPU`          VARCHAR(255)  COMMENT '款号',
                    `季节`         VARCHAR(100),
                    `面料`         VARCHAR(500),
                    `单件用量`     DOUBLE        COMMENT '单位：米',
                    `单件损耗`     DOUBLE        COMMENT '损耗系数',
                    `单件价格`     DOUBLE        COMMENT '单位：元',
                    `面料单价`     DOUBLE        COMMENT '每米单价，单位：元/米',
                    `核价类型`     VARCHAR(100)  COMMENT '飞书核价类型字段，如终版/初版等',
                    `适用部位`     VARCHAR(200)  COMMENT '该面料适用的部位，如主体/里布等',
                    `创建时间`     DATETIME      COMMENT '飞书记录创建时间',
                    `数据更新时间` DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_spu    (`SPU`),
                    INDEX idx_season (`季节`),
                    INDEX idx_fabric (`面料`(100)),
                    UNIQUE KEY uk_spu_fabric (`SPU`, `面料`(100))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='飞书面料核价数据'
            """)

            # 兼容旧表：若列不存在则追加（MySQL 8.0+ 支持 IF NOT EXISTS；
            # 低版本会抛异常，用 try/except 忽略"列已存在"错误）
            for col_sql in [
                "ALTER TABLE `面料核价表` ADD COLUMN IF NOT EXISTS `核价类型` VARCHAR(100) COMMENT '飞书核价类型字段，如终版/初版等' AFTER `面料单价`",
                "ALTER TABLE `面料核价表` ADD COLUMN IF NOT EXISTS `适用部位` VARCHAR(200) COMMENT '该面料适用的部位，如主体/里布等' AFTER `核价类型`",
            ]:
                try:
                    cursor.execute(col_sql)
                except Exception as e:
                    if 'Duplicate column name' in str(e) or '1060' in str(e):
                        pass  # 列已存在，忽略
                    else:
                        raise

            logger.info("  表检查/创建完成（含 核价类型、适用部位 字段）")

    except Exception as e:
        logger.error(f"创建面料核价表失败: {e}", exc_info=True)
        raise


# ────────────────────────────────────────────────────────────────────────────
# 3. insert_fabric_data_batch
# ────────────────────────────────────────────────────────────────────────────

def insert_fabric_data_batch(data_list):
    """
    批量插入面料数据到数据库（INSERT ... ON DUPLICATE KEY UPDATE）。
    新增 核价类型、适用部位 两字段。
    """
    from common.database import db_cursor
    import logging
    logger = logging.getLogger('feishu_fetch_data')

    if not data_list:
        logger.warning("没有数据需要插入")
        return 0

    try:
        with db_cursor(dictionary=False) as cursor:
            sql = """
            INSERT INTO `面料核价表`
                (`SPU`, `季节`, `面料`, `单件用量`, `单件损耗`, `单件价格`, `面料单价`,
                 `核价类型`, `适用部位`, `创建时间`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `季节`     = VALUES(`季节`),
                `单件用量` = VALUES(`单件用量`),
                `单件损耗` = VALUES(`单件损耗`),
                `单件价格` = VALUES(`单件价格`),
                `面料单价` = VALUES(`面料单价`),
                `核价类型` = VALUES(`核价类型`),
                `适用部位` = VALUES(`适用部位`),
                `创建时间` = VALUES(`创建时间`)
            """

            batch_data = [
                (
                    r.get('SPU', ''),
                    r.get('季节', ''),
                    r.get('面料', ''),
                    r.get('单件用量', 0),
                    r.get('单件损耗', 0),
                    r.get('单件价格', 0),
                    r.get('面料单价', 0),
                    r.get('核价类型', ''),   # 新增
                    r.get('适用部位', ''),   # 新增
                    r.get('创建时间', '1970-01-01 00:00:00'),
                )
                for r in data_list
            ]

            cursor.executemany(sql, batch_data)
            affected_rows = cursor.rowcount
            logger.info(f"成功插入/更新 {affected_rows} 条面料记录")
            return affected_rows

    except Exception as e:
        logger.error(f"插入面料数据失败: {e}", exc_info=True)
        raise
