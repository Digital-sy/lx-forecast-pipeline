# pythondata 项目说明

基于 **领星 OpenAPI + MySQL（阿里云 RDS）+ 飞书** 的数据采集、同步与分析管道。

核心目标：把领星业务数据同步到 MySQL，结合飞书运营填报，生成销量预估、采购建议、面料用量预估，为备货决策提供准确数据支撑。

> 说明：费用单创建流程已下线，项目不再创建、作废或上传任何领星费用单。

---

## 一、项目结构

```
pythondata/
├── .env                              # 环境配置（需自行创建）
├── requirements.txt                  # Python 依赖（Python ≥ 3.10）
├── common/                           # 公共模块：配置、数据库、飞书、日志
├── lingxing/                         # 领星 OpenAPI SDK
├── utils/                            # 工具函数
├── jobs/
│   ├── Sync_data/                    # 领星数据同步：Listing、利润报表
│   ├── feishu/                       # 飞书读写、销量预估、面料预估、采购建议
│   └── purchase_analysis/            # 产品、FBA、库存、销量、采购、库存预估
└── scripts/                          # 部署、维护、定时任务入口
```

---

## 二、整体数据流

```
领星 OpenAPI
    │
    ├─→ 产品/Listing/FBA库存/仓库库存/销量/采购单/利润报表
    │         ↓ 同步入库
    │       MySQL (RDS)
    │         │
    ├─→ 飞书多维表（运营填报）
    │     ├─ 运营预计下单量 ────→ 运营预计下单表（MySQL）
    │     ├─ 定制面料参数   ────→ 定制面料参数表（MySQL）
    │     └─ 面料核价数据   ────→ 面料核价表（MySQL）
    │
    └─→ 预测 & 分析管道
          ├─ forecast_sales_improved.py（系统销量预测 v2）
          │       ↓
          │   预测对比表_SKU（MySQL）
          │       ↓
          ├─ generate_procurement_report.py
          │       ↓
          │   建议下单量表（MySQL）→ 飞书多维表
          │       ↓
          ├─ generate_fabric_forecast.py（面料预估 v4）
          │       ↓
          │   面料预估表（MySQL）→ 飞书多维表
          │
          └─ export_fabric_trace.py（面料用量溯源导出）
                  ↓
              /tmp/fabric_XXX_trace.xlsx
```

---

## 三、核心业务流程

### 流程 1：利润报表同步

**入口**：`scripts/run_profit_report_fee_sync.sh`

```
领星 API → fetch_profit_report_msku_daily（上月初至今日）
         → update_profit_report_calculated_fields（更新计算字段）
```

### 流程 2：数据同步与面料预估

**入口**：`scripts/run_data_sync.sh`

```
1. fetch_listing              领星 Listing 入库
2. fetch_fabric_params        飞书定制面料参数入库
3. fetch_feishu_data          飞书面料表入库
4. generate_order_comparison  生成下单对比表
5. generate_fabric_forecast   生成面料预估表（见下节详述）
```

### 流程 3：采购分析

**入口**：`scripts/run_jobs.sh` → `jobs.purchase_analysis.main`

```
fetch_product              领星产品管理（7日增量）
fetch_fba_inventory        领星 FBA 库存
fetch_inventory_details    领星仓库库存明细
fetch_sale_stat_v2_msku_monthly  领星销量统计（MSKU月度）
fetch_purchase             领星采购单
generate_inventory_estimate      生成库存预估表
```

### 流程 4：库存同步

**入口**：`scripts/run_inventory_sync.sh`

```
fetch_inventory_details    领星仓库库存明细
write_inventory_to_feishu  写飞书库存多维表
Shipment_Number            领星货件单号写飞书
```

### 流程 5：采购建议流水线（核心流程）

**入口**：`scripts/run_procurement_pipeline.sh`

```
Step 0: write_order_forecast_to_feishu
        飞书运营预计下单量 → MySQL 运营预计下单表

Step 1: generate_forecast_comparison
        系统销量预测（forecast_sales_improved v2）
        → 预测对比表_SKU（MySQL）
        ※ v2 改进：动态α混合 + 爆发检测 + 环比上限，解决高基数效应虚高问题

Step 2: generate_procurement_report
        系统预测 + FBA库存 + 本地库存（含FBA在途）
        → 建议下单量表（MySQL）→ 飞书多维表
        ※ 修复：FBA库存字段名由 `在途数量` 改为 `在途`，库存不再为0

Step 3: generate_fabric_forecast（面料预估 v4）
        当月：采购单实际下单量（待到货+已完成）× 用量率
        T+1~T+3：建议下单量 × 颜色比例 × 用量率
        → 面料预估表（MySQL）→ 飞书面料预估明细

Step 4: export_procurement_excel
        导出采购建议 Excel 报告
```

---

## 四、面料预估系统详解（v4）

面料预估表是本项目的核心输出，为面料采购提供数据支撑。

### 4.1 设计理念

```
面料消耗时间点 = 下成衣采购单的时刻（而非成衣销售时刻）
→ 用成衣采购单驱动面料需求，消除2-3个月的时间错位
```

### 4.2 字段含义

| 字段 | 来源 | 含义 |
|------|------|------|
| 库存量/条 | 仓库库存明细（按面料编号匹配） | 当前面料库存 |
| 当月已下单消耗/米 | 本月采购单（待到货+已完成）× 用量率 | **确定消耗**，已产生 |
| 当月完整预估/米 | 当月建议下单量 × 颜色比例 × 用量率 | 当月面料需求总量（A方案） |
| 当月剩余预估/米 | 完整预估 - 已下单消耗 | 还需要采购的面料（B方案）|
| T+1月预估/米 | T+1月建议下单量 × 颜色比例 × 用量率 | 下月面料需求预测 |
| T+2月预估/米 | T+2月建议下单量 × 颜色比例 × 用量率 | 下下月面料需求预测 |
| 运营当月预估/米 | 运营预计下单量 × 用量率 | 运营口径参考 |

### 4.3 颜色拆分逻辑

建议下单量是 SPU+店铺 维度，颜色拆分用系统预测比例：

```
某颜色占比 = 该颜色系统预测件数 ÷ 该SPU系统预测总件数
该颜色预估米数 = SPU建议下单量 × 该颜色占比 × 平均用量率
```

### 4.4 建议下单量的库存扣除

建议下单量 = 系统预测 - 成衣FBA可售 - 成衣FBA在途 - 本地可用 - 本地待入库
已充分考虑全渠道库存，避免重复备货。

---

## 五、销量预测算法详解（v2）

**文件**：`jobs/feishu/forecast_sales_improved.py`

### 5.1 预测决策树

```
有去年同期数据？
  是 → 计算趋势因子（近3个月加权同比）
       └─ 爆发检测（趋势因子需钳位 AND 近期环比>30%）
             是 → L3：阻尼增长（真实爆火，放大预测）
             否 → 方案C：动态α混合 → 方案A：环比上限兜底
                   trend>2.0：同比30%+近期70%
                   trend>1.5：同比50%+近期50%
                   trend≤1.5：同比70%+近期30%（接近原逻辑）
  否 → L3：近3月阻尼增长 / L4：SPU兜底
```

### 5.2 关键参数

```python
TREND_FACTOR_MAX       = 3.0    # 趋势因子最大值
EXPLOSIVE_GROWTH_THRESHOLD = 0.3  # 爆发检测近期环比阈值（30%/月）
MOM_CAP_RATIO          = 1.5    # 方案A环比上限（近3月均值×1.5）
ALPHA_HIGH             = 0.3    # trend>2.0时同比权重
ALPHA_MID              = 0.5    # trend>1.5时同比权重
ALPHA_NORMAL           = 0.7    # 正常情况同比权重
```

### 5.3 改进背景

v1 存在高基数效应虚高问题：去年同期基数低（新品起步期），今年同比大幅增长，但算法按同比直接外推导致预测严重虚高（如BX389-BW-S 6月预测4324件，实际1731件，误差+150%）。

v2 加入动态α混合让高增速情况更依赖近期均值，同时区分"基数效应"和"真实爆发"两种场景。

---

## 六、关键修复记录

| 问题 | 原因 | 修复 |
|------|------|------|
| FBA库存在建议下单量里全为0 | `read_inventory()` 里FBA查询用了 `可售数量`、`在途数量`，但实际字段名是 `FBA可售`、`在途` | 修正字段名，FBA和本地库存拆成独立try/except |
| 面料预估表过期数据未清除 | DELETE语句用 `>=` 应该用 `<` | 修正为删除 `< 当月` 的数据 |
| 系统预测虚高 | v1预测算法高基数效应，趋势因子直接外推 | 升级至v2，加入动态α混合和爆发检测 |
| 面料预估时间错位 | 用成衣销量预测推算面料需求，有2-3月时间差 | 改为用建议下单量（成衣采购时间点）驱动 |
| 库存预估表字段名不匹配 | `库存预估表` 是竖表结构，但代码按横表读取 | 改用 `CASE WHEN 库存状态 = '本地可用量'` |

---

## 七、主要文件说明

### `jobs/feishu/`

| 文件 | 说明 |
|------|------|
| `forecast_sales_improved.py` | **系统销量预测算法 v2**，L1~L4四级兜底 + 爆发检测 + 方案C动态α |
| `generate_forecast_comparison.py` | 生成预测对比表_SKU，每日更新当月及未来月份预测 |
| `generate_fabric_forecast.py` | **面料预估表 v4**，采购单驱动当月，建议下单量驱动T+1~T+3 |
| `generate_procurement_report.py` | 生成建议下单量表，写飞书多维表；含FBA+本地双库存 |
| `export_procurement_excel.py` | 导出采购建议 Excel 报告 |
| `export_fabric_trace.py` | **面料用量溯源导出**，生成290等面料的SKU级溯源Excel |
| `write_sales_to_feishu.py` | 销量+库存预估 → 飞书销量预估表 |
| `write_order_forecast_to_feishu.py` | 飞书运营预计下单量 → MySQL |
| `write_inventory_to_feishu.py` | 仓库库存明细 → 飞书 |
| `fetch_fabric_params.py` | 飞书定制面料参数 → MySQL |
| `fetch_feishu_data.py` | 飞书面料表 → MySQL |
| `generate_order_comparison.py` | 生成下单对比表 |
| `Shipment_Number.py` | 领星货件单号 → 飞书 |

### `jobs/purchase_analysis/`

| 文件 | 说明 |
|------|------|
| `main.py` | 采购分析主入口 |
| `fetch_product.py` | 领星产品管理（7日增量） |
| `fetch_fba_inventory.py` | 领星 FBA 库存 |
| `fetch_inventory_details.py` | 领星仓库库存明细 |
| `fetch_sale_stat_v2_msku_monthly.py` | 领星销量统计 MSKU 月度 |
| `fetch_purchase.py` | 领星采购单 |
| `generate_inventory_estimate.py` | 生成库存预估表 |

---

## 八、面料溯源使用说明

**运行方式**：

```bash
cd /opt/apps/pythondata && source venv/bin/activate
python -m jobs.feishu.export_fabric_trace
```

输出文件：`/tmp/fabric_290_trace.xlsx`

下载到本地：

```bash
scp root@SERVER_IP:/tmp/fabric_290_trace.xlsx C:\Users\GA\fabric_290_trace.xlsx
```

**Excel 包含4个Sheet**：

| Sheet | 内容 |
|-------|------|
| 面料预估汇总 | 直接读面料预估表，含总量+带颜色两个维度，权威数据 |
| SKU溯源 | 系统预测件数、颜色内占比、预估面料用量（与汇总一致）、销量+库存 |
| 本月采购单消耗 | 本月实际采购单折算的面料消耗 |
| 核价参数 | 各SPU的单件用量和损耗系数 |

> 注：SKU溯源里的"预估面料用量/米"= 颜色预估总米 × 颜色内SKU占比，与面料预估汇总（带颜色行）数值一致。总量行（含无主面料贡献）与SKU溯源加总可能有小差异，以总量行为准。

---

## 九、快速命令参考

```bash
# 服务器路径
cd /opt/apps/pythondata && source venv/bin/activate

# 同步一次完整流水线
bash scripts/run_procurement_pipeline.sh

# 单独运行某个模块
python -m jobs.feishu.generate_fabric_forecast
python -m jobs.feishu.generate_procurement_report
python -m jobs.feishu.export_fabric_trace

# 拉取最新代码
git pull origin main

# 查看日志
tail -f logs/fabric_forecast.log
tail -f logs/cron_procurement_pipeline.log
```

---

## 十、定时任务建议

推荐每日 02:00 按顺序执行：

```bash
#!/bin/bash
PROJECT_DIR="/opt/apps/pythondata"

"$PROJECT_DIR/scripts/run_profit_report_fee_sync.sh" || true
"$PROJECT_DIR/scripts/run_jobs.sh"                    || true
"$PROJECT_DIR/scripts/run_data_sync.sh"               || true
"$PROJECT_DIR/scripts/run_inventory_sync.sh"          || true
"$PROJECT_DIR/scripts/run_procurement_pipeline.sh"    || true
```

> 实际生产以服务器 crontab / `/etc/cron.daily/` 配置为准。

---

## 十一、环境与维护

- **Python**：≥ 3.10
- **数据库**：MySQL 8.0（阿里云 RDS），host 见 `.env`
- **依赖**：见 `requirements.txt`
- **配置**：项目根目录 `.env`，参考 `common/config.py`
- **日志**：项目下 `logs/`，按任务分文件
- **维护脚本**：
  - `scripts/setup_server.sh`：新机部署
  - `scripts/update_project.sh`：拉代码+更新依赖
  - `scripts/check_status.sh`：检查环境/数据库/crontab

---

## 十二、当前进度

```
✅ 完成
  领星全量数据同步（Listing/FBA/库存/销量/采购/利润报表）
  销量预测算法 v2（动态α + 爆发检测 + 环比上限）
  面料预估系统 v4（采购单驱动 + 建议下单量驱动 + 颜色比例拆分）
  面料预估写飞书（动态月份字段名，T~T+2共3个月滚动展示）
  FBA库存修复（建议下单量现已包含FBA可售+在途）
  面料溯源报告（SKU级别，与汇总口径一致）

🚧 已知差异
  SKU溯源加总 vs 面料预估总量行存在小差异
  原因：总量行包含非主面料SKU的平均用量兜底贡献，带颜色行仅统计主面料SKU
  处理：以总量行为权威，SKU溯源作分析参考

📋 待优化
  当月实际销量回写（销量统计_msku月度数据完整性）
  库存聚合逻辑（面料颜色编号与仓库SKU的精确匹配）
  面料核价表覆盖率（部分SPU-面料组合未配置）
```
