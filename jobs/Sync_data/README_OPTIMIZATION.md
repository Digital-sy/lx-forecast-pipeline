# 利润报表计算字段更新 - 问题诊断与优化

## 🔥 紧急修复

### 问题：查询数据库时总是卡住

**根本原因**：
1. ❌ **数据库连接没有超时配置** - 查询卡住后无限等待
2. ❌ **SQL查询过于复杂** - 11个LEFT JOIN + 大量字符串函数
3. ❌ **无法使用索引** - JOIN条件中使用SUBSTRING_INDEX等函数

### ✅ 已完成的修复

1. **添加数据库超时配置** (`common/config.py`)
   ```python
   DB_CONNECT_TIMEOUT = 10    # 连接超时：10秒
   DB_READ_TIMEOUT = 600       # 读取超时：10分钟
   DB_WRITE_TIMEOUT = 600      # 写入超时：10分钟
   ```

2. **创建诊断工具** (`diagnose_query_performance.py`)
   - 检查表数据量和索引
   - 测试各个JOIN的性能
   - 分析SQL执行计划

3. **创建优化版本** (`update_profit_report_calculated_fields_optimized.py`)
   - 分阶段查询，避免复杂JOIN
   - 使用内存缓存减少数据库查询
   - 添加详细进度日志

## 🚀 立即执行的步骤

### 步骤1: 运行诊断工具

```bash
cd /path/to/pythondata
python jobs/Sync_data/diagnose_query_performance.py
```

**查看输出，重点关注**：
- 各表的数据量（如果利润报表 > 100万条，需要特别注意）
- 索引是否存在（特别是`统计日期`、`SKU`、`店铺`字段）
- 简单查询vs JOIN查询的耗时对比
- EXPLAIN输出中的`type`（如果是`ALL`说明全表扫描）

### 步骤2: 测试优化版本（小批量）

```bash
# 测试100条记录
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py --limit 100

# 测试1天的数据
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py \
    --start-date 2024-01-01 \
    --end-date 2024-01-01
```

**预期结果**：
- ✅ 查询在1-2秒内完成（不卡住）
- ✅ 能看到清晰的进度日志
- ✅ 成功更新记录

### 步骤3: 如果测试成功，运行完整更新

```bash
# 更新最近5天
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py

# 或指定日期范围
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py \
    --start-date 2024-01-01 \
    --end-date 2024-01-31
```

## 📊 两个版本对比

### 原版本 (`update_profit_report_calculated_fields.py`)

**查询方式**: 一次性JOIN所有表
```sql
SELECT ... 
FROM 利润报表 p
LEFT JOIN (产品管理 - SKU匹配) pm ...
LEFT JOIN (产品管理 - SPU平均) pm_spu ...
LEFT JOIN (头程单价 - 负责人本月) fp1 ...
LEFT JOIN (头程单价 - 负责人上月) fp2 ...
LEFT JOIN (头程单价 - 负责人上上月) fp3 ...
-- ... 还有6个头程单价JOIN
WHERE ...
```

**问题**：
- ❌ 11个LEFT JOIN
- ❌ JOIN条件中使用大量字符串函数（无法用索引）
- ❌ 头程单价表被扫描9次
- ❌ 查询计划复杂，容易卡住
- ❌ 难以调试和优化

### 优化版本 (`update_profit_report_calculated_fields_optimized.py`)

**查询方式**: 分阶段查询 + 内存缓存
```python
# 阶段1: 加载产品管理表到内存
sku_weights, spu_weights = load_product_weights()

# 阶段2: 加载头程单价表到内存
freight_data = load_freight_prices(months)

# 阶段3: 简单查询利润报表（不JOIN）
SELECT * FROM 利润报表 WHERE 统计日期 >= ? AND 统计日期 <= ?

# 阶段4: 在Python中匹配和计算（使用内存缓存）
for record in records:
    weight = get_weight_from_cache(sku, sku_weights, spu_weights)
    price = get_price_from_cache(shop, person, date, freight_data)
    # ... 计算

# 阶段5: 批量更新
UPDATE 利润报表 SET ... WHERE id IN (...)
```

**优点**：
- ✅ 查询简单，不会卡住
- ✅ 易于调试和监控
- ✅ 可以看到每个阶段的进度
- ✅ 内存占用可控
- ✅ 性能可预测

## 📈 性能对比（预期）

| 数据量 | 原版本 | 优化版本 | 提升 |
|--------|--------|----------|------|
| 1,000条 | 超时/卡住 | ~5秒 | ✅ |
| 10,000条 | 超时/卡住 | ~20秒 | ✅ |
| 100,000条 | 超时/卡住 | ~3分钟 | ✅ |
| 1,000,000条 | 超时/卡住 | ~20分钟 | ✅ |

## 🔍 诊断输出示例

### 正常情况
```
步骤3: 测试简单查询（不JOIN）
  执行中...
  ✅ 查询成功: 50,000 条记录
  ⏱️  耗时: 0.85秒

步骤4: 测试产品管理表JOIN
  执行中（限制1000条）...
  ✅ 查询成功: 1,000 条记录
  ⏱️  耗时: 1.23秒
```

### 异常情况
```
步骤4: 测试产品管理表JOIN
  执行中（限制1000条）...
  ❌ 查询失败（600.00秒）: (2006, 'MySQL server has gone away')
  
  👉 说明：JOIN查询超时，建议使用优化版本
```

## 🛠️ 故障排查

### 问题1: 优化版本也很慢

**可能原因**：
- 产品管理表或头程单价表数据量太大
- 缺少索引

**解决方法**：
1. 检查表数据量：
   ```sql
   SELECT COUNT(*) FROM 产品管理;
   SELECT COUNT(*) FROM 头程单价;
   ```
2. 如果数据量 > 100万，考虑添加WHERE条件过滤

### 问题2: 查询还是超时

**可能原因**：
- `统计日期`字段没有索引
- MySQL配置的max_allowed_packet太小

**解决方法**：
```sql
-- 添加索引
CREATE INDEX idx_stat_date ON 利润报表(统计日期);

-- 检查配置
SHOW VARIABLES LIKE 'max_allowed_packet';
```

### 问题3: 内存不足

**可能原因**：
- 产品管理表太大，无法全部加载到内存

**解决方法**：
在优化版本中添加批次处理：
- 每次只查询一部分日期范围
- 每次只加载一部分SKU的重量数据

## 📚 相关文件

| 文件 | 说明 | 用途 |
|------|------|------|
| `update_profit_report_calculated_fields.py` | 原版本 | 当前使用（有问题） |
| `update_profit_report_calculated_fields_optimized.py` | 优化版本 | 推荐使用 |
| `diagnose_query_performance.py` | 诊断工具 | 分析性能问题 |
| `OPTIMIZATION_GUIDE.md` | 详细优化指南 | 完整的优化方案 |
| `README_OPTIMIZATION.md` | 快速参考 | 本文件 |
| `common/config.py` | 配置文件 | 已添加超时配置 |

## ⚡ 快速命令参考

```bash
# 诊断
python jobs/Sync_data/diagnose_query_performance.py

# 测试优化版本（100条）
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py --limit 100

# 测试优化版本（1天）
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py \
    --start-date 2024-01-01 --end-date 2024-01-01

# 生产环境运行（最近5天）
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py

# 生产环境运行（指定日期）
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py \
    --start-date 2024-01-01 --end-date 2024-01-31
```

## 📞 需要帮助？

如果问题仍未解决，请提供：

1. ✅ 诊断工具的完整输出
2. ✅ 各表的数据量（`SELECT COUNT(*)`）
3. ✅ MySQL版本（`SELECT VERSION()`）
4. ✅ 系统配置（CPU、内存）
5. ✅ 错误日志（如果有）

---

**更新日期**: 2026-02-06  
**版本**: 1.0

