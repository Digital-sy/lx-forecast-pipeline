# lx-forecast-pipeline

基于 **领星 OpenAPI + MySQL + 飞书多维表** 的销量预测、采购建议与面料需求流水线。

项目负责把领星的产品、销量、库存、采购和利润数据同步到 MySQL，再结合飞书中的运营预计与面料参数，生成未来销量预测、建议下单量、面料预计用量和 Excel 报告。

> 费用单创建流程已下线。本项目不创建、作废或上传领星费用单。

---

## 1. 项目职责

主要解决四个问题：

1. 未来 4 个月每个 SKU / SPU / 店铺预计能卖多少。
2. 系统预测与运营预计差异有多大。
3. 扣除 FBA、本地库存和待到货后，还需要下多少成衣采购单。
4. 成衣采购计划会消耗多少面料，哪些面料需要补货。

整体链路：

```text
领星 OpenAPI / 飞书运营填报
             ↓
          MySQL 数据层
             ↓
     销量预测算法 v4
             ↓
  系统预测 vs 运营预计对比
             ↓
     库存和待到货扣减
             ↓
       建议下单量
             ↓
       面料需求预估
             ↓
 MySQL / 飞书多维表 / Excel
```

---

## 2. 项目结构

```text
lx-forecast-pipeline/
├── .env                         # 环境变量，禁止提交真实密钥
├── requirements.txt            # Python 依赖
├── common/                      # 配置、数据库、日志、飞书客户端
├── lingxing/                    # 领星 OpenAPI 鉴权、签名与请求封装
├── utils/                       # 通用工具
├── jobs/
│   ├── Sync_data/               # Listing、利润报表等同步任务
│   ├── purchase_analysis/       # 产品、库存、销量、采购和库存预估
│   └── feishu/                  # 预测、采购建议、面料预估和飞书输出
└── scripts/                     # 生产流水线、部署、告警和维护入口
```

服务器默认部署路径：

```text
/opt/apps/pythondata
```

---

## 3. 核心生产流水线

入口：

```bash
bash scripts/run_procurement_pipeline.sh
```

实际执行顺序：

```text
预检：py_compile 检查核心 Python 模块语法

1. jobs.feishu.write_order_forecast_to_feishu
   飞书运营预计下单量 → MySQL 运营预计下单表

2. jobs.feishu.generate_forecast_comparison
   历史销量 → 预测算法 v4
   → 预测对比表
   → 预测对比表_SKU

3. jobs.feishu.generate_procurement_report_lx_color
   系统预测 + 库存 + 待到货 + 面料参数
   → 建议下单量表
   → 面料预计用量表
   → 飞书建议下单量 / 面料预计用量 / 面料预估明细
   → 面料预估明细增加“颜色-领星”

4. jobs.feishu.export_procurement_excel
   导出采购建议 Excel
```

任何一步返回非 0 状态都会：

- 停止后续任务；
- 写入 `logs/cron_procurement_pipeline.log`；
- 调用 `scripts/notify_feishu.py` 发送失败告警。

### 主流程安全保护

`generate_forecast_comparison.py` 当前包含以下保护：

- 核心销量源读取失败时直接报错，不再返回部分结果继续运行；
- 销量历史为空时停止覆盖预测表；
- 无法生成大于 0 的 SKU 预测时停止覆盖预测表；
- `预测对比表` 与 `预测对比表_SKU` 的本月及未来数据在同一事务中删除和重写；
- 数据库写入失败时回滚，避免出现“旧数据已删、新数据未写完”的空表状态。

---

## 4. 数据来源与输出表

### 4.1 主要输入表

| 表 | 用途 |
|---|---|
| `销量统计_msku月度` | SKU / 店铺历史月销量，是系统预测核心输入 |
| `SPU季节表` | SPU 的春夏、秋冬、全年属性 |
| `运营预计下单表` | 运营填写的未来月份预计下单量 |
| `FBA库存明细` | FBA 可售和在途库存 |
| `库存预估表` | 本地可用量和本地待到货 |
| `采购单` | 本月已下单成衣数量和最近供应商 |
| `定制面料参数` | 面料编号、每条米数和定制面料范围 |
| `面料核价表` | SPU 对应面料、单件用量和损耗 |

### 4.2 核心输出表

| 表 | 粒度 | 用途 |
|---|---|---|
| `预测对比表` | SPU + 店铺 + 月份 | 系统预测与运营预计对比 |
| `预测对比表_SKU` | SKU + SPU + 店铺 + 月份 | SKU 预测及后续颜色、面料计算 |
| `建议下单量表` | SPU + 颜色 + 店铺 | 扣除库存后的成衣建议下单量 |
| `面料预计用量表` | 面料 | 定制面料预计采购需求汇总 |
| `面料预估表` | 面料 + 颜色 | 库存、待到货、系统和运营口径面料需求 |

---

## 5. 销量预测算法 v4

文件：

```text
jobs/feishu/forecast_sales_improved.py
```

预测范围：本月起未来 4 个月。

### 5.1 趋势因子

使用最近 3 个月今年销量与去年同期销量的加权比值：

```python
TREND_WEIGHTS = {
    "last_1": 0.6,
    "last_2": 0.3,
    "last_3": 0.1,
}

TREND_FACTOR_MIN = 0.3
TREND_FACTOR_MAX = 3.0
```

最近月份权重最高；去年同期为 0 的月份不进入趋势因子。

### 5.2 预测决策树

```text
有去年同期销量，并且趋势因子有效
├─ 爆发检测成立
│  └─ L3：近期增长阻尼预测
├─ 季节款淡季
│  └─ L1：去年同月销量 × 受限增长系数
└─ 旺季 / 过渡月 / 全年款
   └─ L1：同比预测与近 3 月均值动态混合

没有有效同比条件
├─ L3：新品阻尼增长
├─ L3：近期下跌衰减
├─ L3：季节同比或季节调整
├─ L4：SPU 趋势兜底
└─ L5：无有效数据，返回 0
```

### 5.3 季节性

支持：

- 春夏；
- 秋冬；
- 全年。

算法优先从去年销量中动态识别最高的 3 个月作为旺季。动态结果不符合季节规律时，回退到静态旺季：

```python
PEAK_MONTHS_FALLBACK = {
    "春夏": [4, 5, 6, 7],
    "秋冬": [10, 11, 12, 1],
}
```

当目标月份销量相对旺季均值低于 `0.6` 时，按淡季路径计算，避免近 3 个月均值把不同淡季月份预测成相同结果。

### 5.4 新品与成熟度限制

去年有效销售月份越少，允许的增长上限越高：

| 去年有效月份 | 产品阶段参考 | 增长上限 |
|---:|---|---:|
| ≤ 3 | 新品 / 快速成长期 | 3.0 |
| 4～6 | 成长期 | 2.0 |
| > 6 | 成熟期 | 1.5 |

### 5.5 近期下跌识别

当月数据按已完成天数推算整月，同时比较：

```text
当月日均销量 vs 上月日均销量
```

如果当月日均低于上月日均，算法优先走下跌衰减路径，不继续按历史增长率放大。

### 5.6 上行期连续性保护

季节款进入上行期时，会参考上一个预测月，避免预测曲线出现不合理的断崖下降。每个 SKU 的结果同时保留“预测方法”，便于定位它走了哪条决策路径。

---

## 6. 预测对比主流程

文件：

```text
jobs/feishu/generate_forecast_comparison.py
```

处理步骤：

1. 读取当月、近 3 个月和去年同期销量；
2. 当月销量按已完成天数补全整月，仅作为预测输入；
3. 加载 `SPU季节表`；
4. 调用 v4 算法生成 SKU 未来 4 个月预测；
5. 聚合出 SPU + 店铺 + 月份预测；
6. 读取运营预计下单量；
7. 计算差异和差异率；
8. 事务性替换本月及未来月份数据。

差异口径：

```text
差异 = 运营预计下单量 - 系统预测销量
差异率 = 差异 ÷ 系统预测销量
```

系统预测为 0 时，差异率为 `NULL`。

---

## 7. 建议下单量

基础公式：

```text
建议下单量
= MAX(
    0,
    覆盖月份系统预测合计
    - FBA可售
    - FBA在途
    - 本地可用
    - 本地待到货
  )
```

覆盖月份：

| 面料类型 | 默认覆盖月份 |
|---|---:|
| 定制面料 | 3 个月 |
| 现货面料 | 2 个月 |

计算已下沉到：

```text
SPU + 颜色缩写 + 店铺
```

运营预计仍为 SPU + 店铺维度，按各颜色系统预测占比分摊。

---

## 8. 面料需求预估

文件：

```text
jobs/feishu/generate_fabric_forecast.py
jobs/feishu/generate_procurement_report_lx_color.py
```

核心原则：

```text
面料消耗时间点以“下成衣采购单”为准，
而不是以成衣最终销售月份为准。
```

主要字段：

| 字段 | 计算口径 |
|---|---|
| 库存量/条、库存量/米 | 当前面料库存 |
| 待到货量/条、待到货量/米 | 已采购但未到货面料 |
| 当月已下单消耗/米 | 本月有效采购单 × 单件用量 |
| 当月完整预估/米 | 当月系统需求 × 单件用量 |
| 当月剩余预估/米 | 完整预估 - 已下单消耗 |
| T+1、T+2 月预估/米 | 后续月份系统需求 × 单件用量 |
| 运营月份预估/米 | 运营预计下单量 × 单件用量 |

### 颜色-领星

`generate_procurement_report_lx_color.py` 会读取：

```text
lxpm_product_category_snapshot
```

该表由独立项目 `lx-product-m` 维护。脚本按“面料颜色编号 = 领星 SKU”匹配产品名称，并从产品名称中解析类似 `2#黑玛瑙` 的值，写入飞书字段“颜色-领星”。

如果 `lxpm_product_category_snapshot` 不存在，流水线仍可继续，但“颜色-领星”字段为空。

### SKU 颜色销量 → 17 个面料具体颜色用量（只读）

`jobs/feishu/fabric_color_stocking.py` 在现有预测与面料映射后增加一层只读核对：

```text
预测对比表_SKU
  + lxpm_product_category_snapshot（品名、颜色体系）
  + 面料核价表（SPU → 面料、单件用量、单件损耗）
  + 飞书当前有效面料颜色清单
  → 17面料 × 飞书具体颜色 × 未来4个月预估米数
```

米数计算固定为：

```text
预估面料用量（米）= SKU未来4个月预估销量 × 单件用量 × 单件损耗
```

单件用量为空或 0 时不使用其他 SPU 均值，进入“用量参数缺失”。颜色未匹配
但单件用量存在时仍计算米数，并进入“待人工确认SKU”。

自动确认优先级固定为：

1. SKU `颜色中文名` = 清单 `颜色`；
2. SKU `颜色编码` = 清单 `领星新颜色缩写`；
3. 飞书清单显式 `数字#颜色` / `数字-颜色` 色号解析；
4. 使用 `颜色体系 + 颜色编码/中文名` 经 A2023/B2024 编制表精确消歧；
5. 唯一的确定性中文核心/括号内别名；
6. 已启用的历史人工确认映射。

任一级出现多个候选都不会自动选择。波点、圆点、豹纹、印花、花色、格子、
条纹、撞色、黑底、白底、底色、拼色受图案保护，不会强制归到纯色。

全部确定性规则失败后，任务只在同一个面料的飞书清单中生成最多 3 个模糊
候选。候选会处理全半角、括号、末尾“色”、业务后缀、显式色号、括号别名、
常见简繁和标点差异，但只进入“模糊候选审核”，永不计入已确认颜色用量。
默认分数阈值为高 90、中 80、低 70，高置信度还要求第一、第二候选分差至少
10 分；可通过命令参数调整。

为兼容当前主分支尚未把四个颜色字段持久化到 `预测对比表_SKU` 的情况，
兼容读取会显式复用既有规则：`normalize_sku`、`parse_lingxing_color`、
`get_fabric_price_data` 和 `ColorMappingCatalog`。这些上游处理会在“核对摘要”
的 `source_adapter_disclosures` 单独列出；清单直接匹配本身仍保持字面精确。

手工 dry-run：

```bash
python -m jobs.feishu.fabric_color_stocking --dry-run \
  --as-of 2026-07-28 \
  --output-dir /opt/apps/pythondata/exports
```

输出 Excel 包含“颜色用量总览”“飞书颜色用量”“自动归并SKU明细”
“待人工确认SKU”“用量参数缺失”“飞书17面料颜色清单”“模糊候选审核”
“优先补标清单”和“核对摘要”。飞书存在但没有需求的颜色也会以 0 用量输出。
同目录另写 JSON 核对指标。任务只读 MySQL/飞书，不回写远端，也未加入
`run_procurement_pipeline.sh` 的生产执行步骤。

人工审核闭环文件为：

```text
config/fabric_color_manual_mapping.csv
```

审核键是 `面料名 + 原始颜色编码 + 原始颜色中文名 + 颜色体系`，目标使用
飞书记录 ID 和原值共同校验。仅启用记录参与下一次确定性匹配，凭证不进入
CSV 或代码。

飞书清单资源坐标可用以下环境变量覆盖：

```text
FABRIC_COLOR_CATALOG_BASE_TOKEN
FABRIC_COLOR_CATALOG_TABLE_ID
FABRIC_COLOR_CATALOG_VIEW_ID
FABRIC_COLOR_LINKED_TABLE_ID
```

飞书认证继续使用已有 `FEISHU_APP_ID` / `FEISHU_APP_SECRET` 环境变量，凭证不写入代码。

---

## 9. 其他生产入口

### 利润报表同步

```bash
bash scripts/run_profit_report_fee_sync.sh
```

### 基础数据与面料预估同步

```bash
bash scripts/run_data_sync.sh
```

### 采购分析数据采集

```bash
bash scripts/run_jobs.sh
```

主要包括：产品管理、FBA 库存、仓库库存、MSKU 月度销量、采购单和库存预估表。

### 库存同步到飞书

```bash
bash scripts/run_inventory_sync.sh
```

---

## 10. 安装与配置

建议使用 Python 3.10 或更高版本。

```bash
cd /opt/apps/pythondata
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

在项目根目录创建 `.env`。具体字段以 `common/config.py` 为准，至少包括：

```env
LINGXING_HOST=https://openapi.lingxing.com
LINGXING_APP_ID=***
LINGXING_APP_SECRET=***

DB_HOST=***
DB_PORT=3306
DB_USER=***
DB_PASSWORD=***
DB_DATABASE=***

FEISHU_APP_ID=***
FEISHU_APP_SECRET=***
```

所有 Token、App Secret、Webhook 和数据库密码必须通过环境变量或服务器密钥管理维护，不得写入 README、Shell 或 Python 源码。

---

## 11. 运行与验证

### 更新代码

```bash
cd /opt/apps/pythondata
git pull origin main
source venv/bin/activate
pip install -r requirements.txt
```

### 语法检查

```bash
python -m py_compile \
  jobs/feishu/generate_forecast_comparison.py \
  jobs/feishu/forecast_sales_improved.py \
  jobs/feishu/generate_procurement_report_lx_color.py \
  jobs/feishu/export_procurement_excel.py

bash -n scripts/run_procurement_pipeline.sh
```

### 单独运行预测对比

```bash
python -m jobs.feishu.generate_forecast_comparison
```

### 执行完整核心流水线

```bash
bash scripts/run_procurement_pipeline.sh
```

### 查看日志

```bash
tail -f logs/cron_procurement_pipeline.log
```

### 数据库结果检查

```sql
SELECT COUNT(*) AS cnt,
       MIN(统计日期) AS min_date,
       MAX(统计日期) AS max_date
FROM `预测对比表`;

SELECT COUNT(*) AS cnt,
       COUNT(DISTINCT SKU) AS sku_cnt,
       MIN(统计日期) AS min_date,
       MAX(统计日期) AS max_date
FROM `预测对比表_SKU`;

SELECT 月份,
       SUM(系统预测销量) AS system_qty,
       SUM(运营预计下单量) AS operation_qty
FROM `预测对比表`
GROUP BY 月份
ORDER BY MIN(统计日期);
```

---

## 12. 定时任务建议

实际生产以服务器 crontab 或 DolphinScheduler 配置为准。各任务存在上下游依赖，不建议无条件使用 `|| true` 吞掉失败。

推荐顺序：

```text
利润数据同步
  ↓
产品 / 库存 / 销量 / 采购数据同步
  ↓
飞书参数与面料基础数据同步
  ↓
库存同步
  ↓
采购建议核心流水线
```

核心采购流水线自身已实现失败即停止和飞书告警。

---

## 13. 已知事项

1. 预测算法是可解释的业务规则模型，不是训练型机器学习模型。
2. 当月销量补全依赖 `销量统计_msku月度` 中当月数据为“截至昨日累计值”。
3. 运营预计按 SPU + 店铺录入，颜色维度由系统预测占比分摊。
4. 面料核价表缺少单件用量时，对应 SPU 的面料结果可能不完整。
5. “颜色-领星”依赖 `lx-product-m` 的产品快照同步及时性。
6. 飞书报表目前以清空后全量重写为主，执行期间不应并发运行同一输出任务。
