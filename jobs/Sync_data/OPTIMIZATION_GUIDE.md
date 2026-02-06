# 利润报表计算字段更新 - 性能优化指南

## 问题诊断

### 当前问题
`update_profit_report_calculated_fields.py` 在查询数据库时卡住，主要原因：

1. **数据库连接没有超时设置** ✅ 已修复
2. **SQL查询过于复杂**（11个LEFT JOIN + 大量字符串函数）
3. **JOIN条件中使用字符串函数**，无法使用索引
4. **重复扫描同一张表**（头程单价表被JOIN 9次）

### 执行诊断工具

运行诊断脚本来分析具体问题：

```bash
python jobs/Sync_data/diagnose_query_performance.py
```

这个工具会：
- 检查表数据量
- 检查索引情况
- 测试各个JOIN的性能
- 分析SQL执行计划（EXPLAIN）
- 检查MySQL慢查询日志配置

## 优化方案

### 方案1：分阶段查询（推荐 ⭐⭐⭐⭐⭐）

**原理**: 避免复杂的多表JOIN，改为分阶段查询和更新

**优点**:
- 查询简单，易于调试
- 可以看到每个阶段的进度
- 不会出现超时问题
- 内存占用可控

**实施步骤**:

1. **第一阶段**: 只查询利润报表的基础数据
2. **第二阶段**: 批量查询产品管理表，匹配单品毛重
3. **第三阶段**: 批量查询头程单价表，匹配头程单价
4. **第四阶段**: 批量更新计算结果

### 方案2：使用临时表（推荐 ⭐⭐⭐⭐）

**原理**: 预先创建临时表，减少JOIN时的计算

**优点**:
- 避免在JOIN中使用字符串函数
- 可以为临时表创建索引
- 提升查询效率

**实施步骤**:

```sql
-- 1. 创建产品管理临时表（预处理SKU和SPU）
CREATE TEMPORARY TABLE temp_product_weight AS
SELECT 
    TRIM(`SKU`) AS `SKU`,
    SUBSTRING_INDEX(TRIM(`SKU`), '-', 1) AS `SPU`,
    `单品毛重`
FROM `产品管理`
WHERE `SKU` IS NOT NULL 
  AND `SKU` != ''
  AND `单品毛重` IS NOT NULL
  AND `单品毛重` > 0;

-- 为临时表添加索引
CREATE INDEX idx_sku ON temp_product_weight(`SKU`);
CREATE INDEX idx_spu ON temp_product_weight(`SPU`);

-- 2. 创建头程单价临时表（预处理日期和品牌前缀）
CREATE TEMPORARY TABLE temp_freight_price AS
SELECT 
    `店铺`,
    `负责人`,
    CONCAT(SUBSTRING_INDEX(`店铺`, '-', 1), '-') AS `品牌前缀`,
    `头程单价`,
    DATE_FORMAT(`统计日期`, '%Y-%m-01') AS `月份`
FROM `头程单价`
WHERE `统计日期` >= '2024-01-01';

-- 为临时表添加索引
CREATE INDEX idx_shop_person_month ON temp_freight_price(`店铺`, `负责人`, `月份`);
CREATE INDEX idx_shop_month ON temp_freight_price(`店铺`, `月份`);
CREATE INDEX idx_brand_month ON temp_freight_price(`品牌前缀`, `月份`);

-- 3. 主查询（简化的JOIN）
SELECT 
    p.`id`,
    p.`SKU`,
    pw.`单品毛重`,
    fp.`头程单价`
FROM `利润报表` p
LEFT JOIN temp_product_weight pw ON TRIM(p.`SKU`) = pw.`SKU`
LEFT JOIN temp_freight_price fp ON (
    p.`店铺` = fp.`店铺` 
    AND p.`负责人` = fp.`负责人`
    AND DATE_FORMAT(p.`统计日期`, '%Y-%m-01') = fp.`月份`
)
WHERE p.`统计日期` >= %s AND p.`统计日期` <= %s;
```

### 方案3：增加必要的索引（基础 ⭐⭐⭐）

**当前代码已经尝试添加索引，但可能需要额外的复合索引**:

```sql
-- 利润报表表
CREATE INDEX idx_stat_date_shop_person ON `利润报表`(`统计日期`, `店铺`, `负责人`);
CREATE INDEX idx_sku_trim ON `利润报表`((TRIM(`SKU`)));  -- MySQL 8.0+ 支持函数索引

-- 产品管理表
CREATE INDEX idx_sku_weight ON `产品管理`(`SKU`, `单品毛重`);
CREATE INDEX idx_spu_weight ON `产品管理`(`SPU`, `单品毛重`);

-- 头程单价表
CREATE INDEX idx_shop_person_stat_date ON `头程单价`(`店铺`, `负责人`, `统计日期`, `头程单价`);
```

### 方案4：分批处理（已实现，但可优化）

**当前代码已经实现了分批处理，但批次大小可能需要调整**:

```python
# 当前批次大小
query_batch_size = 50000  # 可能太大

# 建议调整为
query_batch_size = 10000  # 或更小
```

## 立即可以采取的措施

### 1. 修改数据库连接超时（✅ 已完成）

我已经更新了 `common/config.py`，添加了超时配置：

```python
# 新增配置
self.DB_CONNECT_TIMEOUT = 10      # 连接超时：10秒
self.DB_READ_TIMEOUT = 600         # 读取超时：10分钟
self.DB_WRITE_TIMEOUT = 600        # 写入超时：10分钟
```

### 2. 运行诊断工具

```bash
# 先诊断问题
python jobs/Sync_data/diagnose_query_performance.py
```

查看输出，重点关注：
- 各表的数据量
- 索引是否存在和有效
- 各个JOIN的耗时
- EXPLAIN输出中的 `type`, `rows`, `key` 列

### 3. 限制查询范围测试

**暂时限制处理的记录数**，测试是否能正常执行：

```bash
# 只处理100条测试
python jobs/Sync_data/update_profit_report_calculated_fields.py --limit 100

# 只处理1天的数据
python jobs/Sync_data/update_profit_report_calculated_fields.py \
    --start-date 2024-01-01 \
    --end-date 2024-01-01
```

### 4. 监控查询进度

在 `main()` 函数的查询执行部分添加更多日志：

```python
# 第1664行附近
logger.info("  正在执行查询...")
logger.info(f"  SQL前500字符: {sql[:500]}")  # 添加：显示SQL
start_time = time.time()

# 添加：每5秒输出一次进度
import threading
def log_progress():
    elapsed = time.time() - start_time
    logger.info(f"  查询执行中... 已等待 {elapsed:.0f} 秒")
    if cursor._executed:  # 如果查询还在执行
        threading.Timer(5.0, log_progress).start()

log_progress()
cursor.execute(sql, (start_date, end_date))
```

### 5. 查看MySQL慢查询日志

如果启用了慢查询日志，查看具体的慢查询：

```sql
-- 启用慢查询日志
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 2;  -- 记录超过2秒的查询

-- 查看慢查询日志文件位置
SHOW VARIABLES LIKE 'slow_query_log_file';
```

## 推荐的优化顺序

1. ✅ **添加数据库超时配置**（已完成）
2. 🔄 **运行诊断工具**，确认问题所在
3. 🔄 **限制查询范围**，测试小批量数据
4. 🔄 **分析EXPLAIN输出**，确认索引使用情况
5. 🔄 **实施方案1或方案2**（分阶段查询或临时表）
6. 🔄 **调整批次大小**，找到最优值
7. 🔄 **添加更多监控日志**，实时查看进度

## 性能基准

### 预期性能（优化后）

| 数据量 | 预期耗时 | 备注 |
|--------|----------|------|
| 1,000条 | < 5秒 | 测试环境 |
| 10,000条 | < 30秒 | 小批量 |
| 100,000条 | < 5分钟 | 中批量 |
| 1,000,000条 | < 30分钟 | 大批量 |

### 性能瓶颈识别

如果某个阶段特别慢，对应的优化方法：

| 慢的阶段 | 可能原因 | 优化方法 |
|----------|----------|----------|
| 查询阶段 | 复杂JOIN | 使用临时表或分阶段查询 |
| 数据获取 | 返回字段太多 | 减少SELECT字段数量 |
| 计算阶段 | Python计算慢 | 考虑在SQL中直接计算 |
| 更新阶段 | 批次太小 | 增大批次大小 |

## 需要帮助？

如果遇到问题，请提供：

1. 诊断工具的完整输出
2. 各表的数据量（`SELECT COUNT(*)`）
3. 查询卡住时的等待时间
4. MySQL版本信息（`SELECT VERSION()`）
5. 系统配置（CPU、内存、磁盘）

## 联系信息

创建日期: 2026-02-06
最后更新: 2026-02-06

