# 面料预估表改进版 - GitHub 替换和测试步骤

## 📋 文件清单

- **generate_fabric_forecast_v2.py** - 改进版代码（41KB）

## 🔄 GitHub 替换步骤

### Step 1：拉取最新代码
```bash
cd /path/to/pythondata-github
git pull origin main
```

### Step 2：替换文件
```bash
# 备份原文件（以防需要回滚）
cp jobs/feishu/generate_fabric_forecast.py jobs/feishu/generate_fabric_forecast.py.backup_v1

# 用新版本替换
cp /path/to/generate_fabric_forecast_v2.py jobs/feishu/generate_fabric_forecast.py
```

### Step 3：检查语法
```bash
python -m py_compile jobs/feishu/generate_fabric_forecast.py
# 无输出表示OK
```

### Step 4：提交 GitHub
```bash
git add jobs/feishu/generate_fabric_forecast.py
git commit -m "改进面料预估表：增加过期数据过滤、当月分离、库存分列显示"
git push origin main
```

---

## 🧪 服务器测试步骤

### Step 1：拉回服务器
```bash
cd /opt/apps/pythondata
git pull origin main
```

### Step 2：运行单次测试
```bash
source venv/bin/activate
python -m jobs.feishu.generate_fabric_forecast
```

**预期输出**：
```
================================================================================
面料预估表生成任务（v2 - 改进版）
================================================================================
检查/创建面料预估表...
✓ 面料预估表检查完成
读取定制面料参数...
  读取到 X 种定制面料
读取面料核价表...
  读取到 Y 个 SPU-面料组合
读取运营预计下单表...
  读取到 Z 个 SKU+日期
读取预测对比表_SKU（系统预估）...
  读取到 W 个 SKU+日期
过滤前系统预测数据：W 条
过滤后系统预测数据：W-X 条    ← 【关键】应该减少过期数据
读取当月实际销量（销量统计_msku月度）...
  读取到 N 个 (SKU, 店铺) 的实际销量
...
生成 M 条记录
✓ 成功写入 M 条数据
================================================================================
任务完成
================================================================================
```

### Step 3：验证数据库数据

```bash
# 登录数据库
mysql -h rm-wz91237y91oasq45fco.mysql.rds.aliyuncs.com -u SYSJ001 -p

# 进入库
USE lingxing;

# 查看最新数据
SELECT 统计类型, 月份, COUNT(*) as cnt, 
       MAX(统计日期) as latest
FROM `面料预估表`
WHERE 统计日期 >= DATE_SUB(NOW(), INTERVAL 1 DAY)
GROUP BY 统计类型, 月份
ORDER BY 月份 DESC;

# 样本查询：看某块料的当月 vs 未来数据
SELECT 统计类型, 月份, 面料, 库存量_条, 已消耗用量_米, 剩余预计用量_米, 待到货量_条
FROM `面料预估表`
WHERE 面料 = '290涤双磨'
  AND 统计日期 >= DATE_SUB(NOW(), INTERVAL 2 DAY)
ORDER BY 月份, 统计类型;
```

**预期结果**：
- 总月份数应该减少（删除了过期数据）
- 当月应显示 "已消耗用量/米" 和 "剩余预计用量/米" 两列
- 未来月份的库存 = 0，待到货 = 0

### Step 4：对标历史数据

```sql
-- 看4月、5月是否被过滤掉了
SELECT DISTINCT DATE_FORMAT(统计日期, '%Y-%m') as month, COUNT(*) as cnt
FROM `面料预估表`
WHERE 统计日期 >= '2026-04-01'
GROUP BY DATE_FORMAT(统计日期, '%Y-%m')
ORDER BY month;

-- 查看某块料的当月实际销量 vs 预测
SELECT 面料, 统计类型, 月份, 系统预估下单量, 
       已消耗用量_米, 剩余预计用量_米
FROM `面料预估表`
WHERE 面料 = '290涤双磨'
  AND 统计类型 LIKE '%-当月'
ORDER BY 月份 DESC;
```

---

## ⚙️ 生产环境上线

### 确认无误后，加入定时任务

在 `/opt/apps/pythondata/scripts/run_data_sync.sh` 中，确保有：
```bash
python -m jobs.feishu.generate_fabric_forecast
```

或者执行整个流水线：
```bash
cd /opt/apps/pythondata && bash scripts/run_data_sync.sh
```

### 监控日志
```bash
tail -f /opt/apps/pythondata/logs/fabric_forecast.log
```

---

## 🔙 回滚步骤（如遇问题）

```bash
# 恢复原文件
cp jobs/feishu/generate_fabric_forecast.py.backup_v1 jobs/feishu/generate_fabric_forecast.py

# 提交回滚
git add jobs/feishu/generate_fabric_forecast.py
git commit -m "回滚：恢复原版面料预估表"
git push origin main

# 重新运行
python -m jobs.feishu.generate_fabric_forecast
```

---

## 📊 改进点总结

| 功能 | 旧版 | 改进版 | 备注 |
|---|---|---|---|
| 过期数据处理 | 保留4月、5月的预测 | 删除过期数据 | 减少虚高预测 |
| 当月库存显示 | 库存+预测 混杂 | 库存 \| 已消耗 \| 剩余预计 \| 待到货 | 明细化，逻辑清晰 |
| 未来月份 | 显示库存 | 不显示库存 | 规划视角 |
| 实际销量 | 无 | 读销量统计_msku月度 | 当月分离计算 |
| 数据准确性 | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | 更真实反映库存消耗 |

---

## ❓ 常见问题

**Q1：为什么看不到4月、5月的数据？**  
A：那些数据已过期，改进版自动删除了。如需历史数据，查看备份或前版本日志。

**Q2："已消耗用量"和"库存"为什么加起来不等于"预计总量"？**  
A：因为剩余预计包含了后续月份的预测，不是当前库存。看当月数据时，剩余 = (预测 - 已销) + 后续月份。

**Q3：某块料的剩余预计为负数？**  
A：说明实际销量超过了预测。这是正常的，反映出预测的保守性。库存会补充。

**Q4：能否看到按天变化的库存？**  
A：当前版本按月聚合。如需按天，需要扩展销量统计表支持日度统计。

---

## 📞 技术支持

如有问题，检查：
1. Python 环境（3.8+）
2. MySQL 连接
3. 各源表是否存在（销量统计_msku月度、预测对比表_SKU等）
4. 日志文件：`logs/fabric_forecast.log`
