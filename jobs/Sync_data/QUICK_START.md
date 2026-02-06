# 🚀 利润报表性能优化 - 快速执行指南

## 问题
`update_profit_report_calculated_fields.py` 查询数据库时卡住

## 解决方案
✅ 已添加数据库超时配置  
✅ 已在数据采集时自动创建性能索引  
✅ 已创建优化版更新脚本

---

## 立即执行（3步）

### 第1步：确保索引已创建 ⏱️ 约1分钟

```bash
# 运行数据采集脚本（任意日期），会自动检查并添加索引
python jobs/Sync_data/fetch_profit_report_msku_daily.py \
    --start-date 2024-01-01 --end-date 2024-01-01
```

**看到这些输出说明成功**:
```
✅ 已添加索引: idx_stat_date (统计日期)
✅ 已添加索引: idx_shop (店铺)
✅ 已添加索引: idx_sku (SKU)
✅ 已添加索引: idx_shop_person_date (店铺, 负责人, 统计日期)
✅ 已添加索引: idx_date_shop (统计日期, 店铺)
```

或者：
```
所有性能优化索引都已存在
```

---

### 第2步：测试优化版本 ⏱️ 约10秒

```bash
# 测试100条数据
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py --limit 100
```

**预期**: 不卡住，看到进度日志，成功完成

---

### 第3步：生产环境运行 ⏱️ 根据数据量

```bash
# 更新最近5天数据
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py
```

**或指定日期范围**:
```bash
python jobs/Sync_data/update_profit_report_calculated_fields_optimized.py \
    --start-date 2024-01-01 --end-date 2024-01-31
```

---

## 可选：运行诊断工具

```bash
python jobs/Sync_data/diagnose_query_performance.py
```

**检查内容**:
- ✅ 索引是否存在
- ✅ 查询性能是否正常
- ✅ 是否有全表扫描

---

## 验证索引已创建

### 方法1: MySQL命令

```sql
SHOW INDEX FROM 利润报表;
```

**应该看到**:
- PRIMARY (id)
- uk_date_shop_msku_asin
- idx_stat_date ⭐ 新增
- idx_shop ⭐ 新增
- idx_sku ⭐ 新增
- idx_shop_person_date ⭐ 新增
- idx_date_shop ⭐ 新增

### 方法2: 查看日志

运行数据采集脚本时，日志会显示索引创建情况

---

## 性能对比

| 场景 | 优化前 | 优化后 |
|------|--------|--------|
| 查询卡住 | ✅ 是 | ❌ 否 |
| 查询超时 | ✅ 是 | ❌ 否 |
| 处理速度 | 极慢/无法完成 | 快速完成 |

---

## 故障排查

### 如果仍然卡住

1. **检查索引**
   ```sql
   SHOW INDEX FROM 利润报表 WHERE Key_name LIKE 'idx_%';
   ```

2. **检查数据量**
   ```sql
   SELECT COUNT(*) FROM 利润报表;
   ```

3. **运行诊断工具**
   ```bash
   python jobs/Sync_data/diagnose_query_performance.py
   ```

---

## 详细文档

- 📖 [完整修复总结](PERFORMANCE_FIXES_SUMMARY.md)
- 📖 [优化指南](README_OPTIMIZATION.md)

---

**更新时间**: 2026-02-06

