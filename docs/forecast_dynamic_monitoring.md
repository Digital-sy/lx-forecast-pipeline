# 销量预测动态监控系统（Shadow V1）

## 1. 目标

把当前“离线回测 → 人工看结果 → 再调算法”的方式，升级为长期运行的数据闭环：

```text
源数据每日同步
    ↓
每日真实快照（库存 / 销量 / 流量 / CVR / 当前预测）
    ↓
NEW_VISIBLE Breakout Watch
    ↓
预测结果永久保留 issue_date
    ↓
目标月成熟后自动评估 WAPE / Bias
    ↓
Champion vs Challenger
    ↓
人工确认后再升级生产模型
```

Shadow V1 不修改现有生产预测算法，不覆盖 `预测对比表`，只新增 `forecast_*` 表。

---

## 2. 为什么必须先做动态快照

### 2.1 库存

当前 `ods_db.ods_lx_fba_warehouse_detail` 是当前态库存，不具备历史日快照。

因此：

- 不能用今天的库存去回测过去月份；
- 不能人为伪造过去库存；
- 从 Shadow V1 启用当天开始，每天保存一份真实库存点时快照；
- 30 / 60 / 90 天后逐步形成可用于新品预测的库存历史。

### 2.2 流量

产品表现数据若本身带 `dt` 日维度，则历史 Sessions / 销量可以马上严格按历史 snapshot 重建。

Shadow V1 自动识别日表现源及字段。当前候选表：

- `ods_db.ods_lx_product_performance_asin`
- `ods_db.ods_lx_product_performance`

自动识别候选字段：

- 日期：`dt / stat_date / report_date / date`
- 店铺ID：`sid / store_id`
- SKU：`sku / seller_sku / msku`
- 销量：`volume / units / units_ordered / ...`
- 流量：`sessions / session / sessions_total / ...`

如果 Sessions 字段没有识别出来，系统会明确 WARN，不会把流量静默填 0。

---

## 3. 新增表

### 3.1 `forecast_inventory_snapshot_daily`

每日当前 FBA 库存快照。

保留原始库存组成：

- FBA可售
- FBA预留
- 待调仓
- 标发在途
- 入库中

同时保存当前业务口径：

```text
FBA总库存 = 可售 + 预留 + 待调仓 + 标发在途 + 入库中
FBA可用库存 = 可售 + 预留 + 待调仓 + 入库中
```

原始分项必须保留，后续如果确认 `afn_reserved_quantity` 与 `reserved_fc_transfers` 有包含关系，可以重新计算，不丢历史。

### 3.2 `forecast_feature_snapshot_daily`

SPU × 店铺 × snapshot_date 的点时特征。

目前保存：

```text
sales_7d
sales_prev_7d
sales_14d
sales_prev_14d
sales_30d

sessions_7d
sessions_prev_7d
sessions_14d
sessions_prev_14d
sessions_30d

cvr_7d
cvr_prev_7d
cvr_14d
cvr_30d

sales_growth_7d
sessions_growth_7d
cvr_ratio_7d

first_sale_date
months_since_first_sale
forecastability

fba_available_inventory
inventory_days_supply
```

`forecastability` 目前用于影子监控：

- 首销月龄 <= 3个月：`NEW_VISIBLE`
- 更老：`ESTABLISHED`
- 首销无法确认：`UNKNOWN`

与 V5 回测定义保持同一方向：新品是快照时可见销售历史 <=3个月的 SPU。

### 3.3 `forecast_prediction_snapshot`

这是整个系统最重要的新表之一。

每天永久记录：

```text
issue_date
SPU
shop
target_month
horizon
model_name
model_version
forecast_qty
forecastability
```

以后不再出现“历史预测被当前重跑覆盖，无法知道当时究竟预测多少”的问题。

当前 Shadow V1 会把生产 `预测对比表` 复制进来，标记：

```text
model_name = production_current
model_version = legacy_v4_live
```

V5 真正接入生产后，应把 `model_version` 改为不可变版本号/commit SHA。

### 3.4 `forecast_breakout_monitor_daily`

只监控 `NEW_VISIBLE`。

V0 是透明规则监控，不是假装成训练好的概率模型：

- 7日销量加速
- 7日 Sessions 加速
- CVR 是否同步保持/改善
- 当前7日销量水平
- 库存覆盖天数

输出：

```text
breakout_score
risk_level = HIGH / MEDIUM / LOW
reason_code
monitor_version = RULE_V0_MONITOR_ONLY
```

`breakout_probability` V0 保持 NULL。只有未来通过严格 OOS 训练和校准后，才允许写概率。

---

## 4. 每日执行顺序

```text
领星 / 产品表现 / FBA 数据同步完成
        ↓
run_forecast_monitoring_daily.sh
        ↓
01 FBA库存快照
        ↓
02 7/14/30日销量/流量/CVR特征
        ↓
03 当前生产预测issue-date快照
        ↓
04 NEW_VISIBLE breakout watch
        ↓
05 飞书摘要
```

每日脚本：

```bash
scripts/run_forecast_monitoring_daily.sh
```

已经包含：

- `flock` 防并发重复运行；
- Python `py_compile` 预检；
- 日志；
- 失败飞书通知；
- 成功/Breakout 摘要。

---

## 5. 第一次上线步骤

### 第一步：只读检查数据源

先不要创建任何表：

```bash
cd /opt/apps/pythondata

git fetch origin

git show origin/main:scripts/audit_forecast_monitoring_sources.py \
  > scripts/audit_forecast_monitoring_sources.py

git show origin/main:jobs/forecast_monitoring/__init__.py \
  > jobs/forecast_monitoring/__init__.py

git show origin/main:jobs/forecast_monitoring/daily_monitor.py \
  > jobs/forecast_monitoring/daily_monitor.py

./venv/bin/python -m py_compile \
  scripts/audit_forecast_monitoring_sources.py \
  jobs/forecast_monitoring/daily_monitor.py

./venv/bin/python scripts/audit_forecast_monitoring_sources.py
```

必须重点确认输出中：

```text
date
sid
sku
sales
sessions
```

都映射到正确字段。

如果 `sessions=None`，先不要挂正式 Cron，先把实际流量字段名加入 `COLUMN_CANDIDATES['sessions']`。

### 第二步：手工跑一次 Shadow V1

确认数据源后：

```bash
./venv/bin/python -m jobs.forecast_monitoring.daily_monitor
```

第一次正常执行会创建四张 `forecast_*` 表，并写当天真实快照。

### 第三步：核对当天数据

```sql
SELECT snapshot_date, COUNT(*)
FROM forecast_inventory_snapshot_daily
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 5;

SELECT
    snapshot_date,
    as_of_date,
    forecastability,
    COUNT(*) AS spu_cnt,
    SUM(sales_7d) AS sales_7d,
    SUM(sessions_7d) AS sessions_7d
FROM forecast_feature_snapshot_daily
GROUP BY snapshot_date, as_of_date, forecastability
ORDER BY snapshot_date DESC, forecastability;

SELECT
    issue_date,
    horizon,
    COUNT(*) AS cnt,
    SUM(forecast_qty) AS forecast_qty
FROM forecast_prediction_snapshot
GROUP BY issue_date, horizon
ORDER BY issue_date DESC, horizon;

SELECT
    snapshot_date,
    risk_level,
    COUNT(*) AS cnt
FROM forecast_breakout_monitor_daily
GROUP BY snapshot_date, risk_level
ORDER BY snapshot_date DESC, risk_level;
```

### 第四步：再挂 Cron

先确认服务器时区和源数据同步完成时间：

```bash
timedatectl
crontab -l
```

示例：如果所有源数据每天 08:00 前已经稳定，08:30 再跑监控：

```cron
30 8 * * * /bin/bash /opt/apps/pythondata/scripts/run_forecast_monitoring_daily.sh
```

不要机械照抄 08:30；应放在实际 ODS/FBA/产品表现同步完成之后 20–30 分钟。

---

## 6. 每日运行 ≠ 每日重训

动态系统分三种节奏。

### 每日

```text
采集快照
更新可见特征
重新计算当前预测
Breakout Watch
保存预测issue date
```

### 每周

只做监控层校准/报告：

- Breakout HIGH/MEDIUM 的后续销量表现；
- 流量信号命中率；
- 库存约束是否导致高风险款未爆发；
- 数据源缺失率/延迟。

不自动替换生产 Champion。

### 每月

目标月完整结束后，才产生 H0/H1/H2/H3 的正式标签。

自动评估：

```text
WAPE
Bias
ABC-A WAPE
ESTABLISHED
NEW_VISIBLE
COLD_NO_HISTORY
```

再做 Champion vs Challenger。

---

## 7. Champion / Challenger 原则

当前阶段建议：

```text
ESTABLISHED Champion = A16
NEW_VISIBLE safe baseline = A3
COLD_NO_HISTORY = 等待上新计划输入
```

未来 Breakout 模型作为 Challenger 并行跑，不直接改采购。

建议晋级条件至少包括：

```text
最近多个成熟目标月 H2/H3 WAPE 均改善
Bias 控制在合理范围
ABC-A 不明显恶化
不是靠单月/单类目获胜
样本量足够
```

Challenger 达标后只标记 `PROMOTION_CANDIDATE`，人工确认后才改生产。

---

## 8. 下一阶段：Breakout V1

等日流量字段确认后，优先回测：

```text
sales_7d / previous_7d
sales_14d / previous_14d
sessions_7d / previous_7d
sessions_14d / previous_14d
CVR变化
首销月龄
品类
季节
当前销量层级
```

库存因为没有历史，只能从 Shadow V1 启用日之后加入严格 OOS 训练。

Breakout V1 不直接预测销量，建议拆成：

```text
P(Breakout)
        ×
Breakout后的条件销量分布
```

最后形成：

```text
forecast_low
forecast_base
forecast_high
breakout_probability
```

用于采购风险而不是只给一个单点值。

---

## 9. 生产接入顺序

不要一次性替换现有采购流水线。

建议顺序：

```text
阶段1  Shadow快照（当前版本）
阶段2  Breakout V1只监控
阶段3  Champion/Challenger自动评估
阶段4  NEW_VISIBLE预测进入影子采购
阶段5  人工确认后再进入正式销量预测
阶段6  SPU → SKU/颜色/尺码 → 面料消耗
```

核心原则：先积累真实 point-in-time 数据，再允许模型“学习”。
