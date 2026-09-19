# 面料颜色治理与预计下单进度（2026-08-10）

## 已完成

- SKU 颜色体系严格改为 SKU 自身显式 A2023/B2024；空、无效、冲突统一待定。
- 禁止通过 SPU、同款其他 SKU、颜色代码、飞书颜色、领星新颜色或人工映射反推 A/B。
- 第一轮历史人工映射继续保留：60 条读取、55 条启用、5 条禁用。
- 新增 SPU 级人工映射，主键为 `面料名 + 原始颜色编码 + SPU`。
- 当前 SPU 映射：529 行、529 唯一主键、0 冲突。
- SPU 人工映射优先于自动规则和历史四字段映射，仅决定最终飞书颜色，不改变颜色体系。
- 模糊候选保持人工审核用途，不进入最终已确认颜色。
- 已生成正式飞书颜色版面料预计下单表，并接入采购流水线第 5 步。
- 新增业务版 21 列主表输出，月份随运行月份自动滚动。
- 已同步当前 `main`（`76833a6`）到 `codex/fabric-color-forecast`，同步点分支 `behind=0`。

## 生产 dry-run 验证

### 接入 SPU 映射前

- 可确定总用量：579,532.96 m
- 已确认颜色用量：474,055.68 m
- 已确认覆盖率：81.80%
- 待人工确认：105,477.28 m
- 模糊候选：9,961.65 m

### 接入 529 条 SPU 映射后

- 可确定总用量：579,532.96 m
- 已确认颜色用量：508,770.19 m
- 已确认覆盖率：87.79%
- 待人工确认：70,762.77 m
- SPU 人工命中：34,937.80 m

守恒结果：总面料需求差异 0.00 m。

其中 223.29 m 为原已自动确认、后被 SPU 人工规则修正颜色：

- SPU：BX285
- 原始颜色编码：TG
- 颜色体系：A2023
- 面料：013仿棉拉架-优化
- 原自动颜色：37#灰褐色
- 最终人工确认颜色：70#嫩绿色
- 涉及 5 个尺码 SKU

因此：

`34,937.80 = 34,714.51（待确认→确认） + 223.29（已确认→修正颜色）`

## 最终预计下单表

当前正式导出模块：

- `jobs/feishu/export_fabric_color_order_forecast_final.py`
- `jobs/feishu/export_fabric_color_order_forecast_business.py`

正式 2026-08-10 17:10 运行结果：

- 主表：277 行
- 具体飞书颜色：262 行
- 面料总量：15 行
- 待确认颜色：164 行
- 4 月颜色覆盖率：89.06%
- 输出：`面料预估表_最终版_20260810_171026.xlsx`

正式表使用生产采购口径：

`建议下单量 → SKU 颜色/尺码比例 → 面料单耗 → 最终飞书颜色 → 库存/在途`

因此该 89.06% 与只读销量 dry-run 的 87.79% 不要求完全一致。

## 业务版主表 21 列

1. SKU
2. 面料颜色编号
3. 面料
4. 颜色
5. 领星颜色
6. 库存量/米
7. 待到货量/米
8. 统计类型
9. 面料编号
10. 颜色缩写
11. 库存量/条
12. 待到货量/条
13. 用量信息缺失SPU
14. 当月完整预估/米
15. 当月已下单消耗/米
16. 当月剩余预估/米
17. T+1月预估/米
18. T+2月预估/米
19. 运营当月预估/米
20. 运营T+1月预估/米
21. 运营T+2月预估/米

字段口径：

- `颜色`：最终飞书颜色
- `领星颜色`：飞书 `领星新颜色名称`
- `颜色缩写`：飞书 `领星新颜色缩写`
- `面料颜色编号`：`面料编号-领星新颜色缩写`

## 库存原则

颜色库存只按 `面料编号 + 飞书领星新颜色缩写` 精确匹配，同一飞书颜色只分配一次，禁止跨颜色重复扣库存。

面料总量库存继续独立聚合，不由颜色库存反推。

## 每日自动更新设计（2026-08-19）

服务器当前基础数据任务已确认正常执行：

- 22:30 采购分析：产品、FBA库存、仓库库存、月销量、采购单、库存预估；
- 02:00 基础数据同步；
- 08:30 / 10:30 / 12:30 / 14:30 / 16:30 / 18:30 库存与货件同步。

最终业务面料预测改为仅在每日 **08:30 库存同步成功后** 执行：

`08:30库存同步 → 最新预测对比 → 建议下单 → SPU人工颜色映射 → 最终21列表 → 飞书“面料预估明细”`

新增：

- `scripts/run_morning_inventory_and_forecast.sh`：串联 08:30 库存同步和最新采购/面料预测流水线；库存失败则不使用旧库存继续预测。
- `export_fabric_color_order_forecast_business.py --write-feishu`：将最终业务 21 列全量覆盖写入飞书。
- `run_procurement_pipeline.sh` 第 5 步默认写入飞书 `面料预估明细`。
- 第 3 步旧版“面料预估明细”写入在最终业务表模式下跳过，避免同一轮任务重复改表结构。

飞书写入前会拒绝空结果清表；写入后校验实际写入条数必须与业务主表行数一致。

## 当前分支与 PR

- 分支：`codex/fabric-color-forecast`
- PR：#9
- PR 保持 Draft
- 已合入当前 main：`76833a6 Fix cron shipment incremental module import`
- 同步 merge commit：`8fd7cf4 Merge latest main into fabric color forecast branch`
- 同步后 `main...codex/fabric-color-forecast`：`behind 0`，仅保留本功能分支自身改动。

主要新增提交：

- `0ba71fa` Add SPU-level manual fabric color mapping dry-run
- `ee4337d` Test SPU-level manual fabric color mapping
- `e0bfc80` Add final Feishu-color fabric order forecast export
- `b8be3a5` Use final Feishu-color fabric order forecast in procurement pipeline
- `8f7cb72` Pass shared manual color mappings to final forecast export
- `dd77285` Export final fabric forecast with business 21-column layout
- `ad6f485` Use business-layout final fabric forecast export
- `ecfb55d` Document fabric color forecast progress for 2026-08-10
- `8fd7cf4` Merge latest main into fabric color forecast branch
- `4bc53c2` Write final business fabric forecast to Feishu
- `c17affe` Skip legacy fabric detail write when final business table owns Feishu output
- `58d2a83` Publish final fabric forecast to Feishu in procurement pipeline
- `f3a8070` Run final fabric forecast after 08:30 inventory sync

## 待完成

- 在生产 `/opt/apps/pythondata` 部署当前分支并配置 08:30 新链路 cron。
- 首次自动写飞书后核对：行数、总米数、库存唯一归属、待确认颜色、月份字段。
- 将 `fabric_color_stocking_spu` 正式内聚到 `fabric_color_stocking`，减少双模块维护。
- 验证完成后再将 PR #9 从 Draft 转 Ready 并合并 main。

第二轮 A 历史 24 组没有可验证原始产物，不再推断恢复；只按当前可验证规则继续治理。
