# NEW_VISIBLE V1 Stage-1 研究决策（2026-10-07）

> 状态：Research-only。不得替换 PROD-V4、A16 或现有采购链路。

## 1. 当前冻结结论

- Label：`NV-PERSIST-750-v1`
- 模型族：LightGBM
- 当前研究模型：`NV-ML-V1-B-CORE`
- 训练方式：四店 pooled global model，不做分店独立模型
- 核心特征：稳定行为特征 CORE + `store_name` + `launch_month` + `snapshot_month`
- 首版不纳入：price / ads / promo
- 评估：严格 temporal OOS；同一 `store × SPU` launch 不允许跨 train/test；训练标签窗口必须在 test_start 前完全成熟

## 2. Label 定义

`NV-PERSIST-750-v1`：

```text
future_sales_30d >= 750
AND
future_sales_first14 > 0
AND
future_sales_second14 >= 0.8 * future_sales_first14
```

业务含义：未来 30 天达到值得追踪/追单的体量，同时第二个 14 天没有明显塌陷，过滤短期 spike。

## 3. CORE 特征范围

CORE 只使用 point-in-time 可获得、历史覆盖稳定的行为特征：

- age_days
- 3/7/14/30 天销量及前周期销量
- 3/7/14/30 天 Sessions 及前周期 Sessions
- 3/7/14/30 天 CVR
- sales/session/CVR growth
- positive days / up days
- 7 天 slope
- 7 天 coefficient of variation
- 7 天 max-day-share
- 分类特征：store、launch month、snapshot month

首版明确排除：

- `avg_price_7d/30d`
- clicks / impressions
- ad spend / orders / sales
- promotion units

原因不是这些特征永远无用，而是历史覆盖存在明显时间漂移，首版优先保证可迁移性和解释稳定性。

## 4. 消融结论

### Pooled strict temporal OOS

| Feature set | PR-AUC | ROC-AUC | Brier | LogLoss |
|---|---:|---:|---:|---:|
| CORE | 0.460984 | 0.827792 | 0.113639 | 0.361966 |
| CORE_PROMO | 0.469743 | 0.829372 | 0.112302 | 0.358988 |
| ENRICHED_NO_PRICE | 0.471047 | 0.824760 | 0.113597 | 0.364884 |
| ENRICHED_ALL | 0.469008 | 0.826808 | 0.113901 | 0.365092 |

虽然增强特征 pooled PR-AUC 略高，但优势只有约 0.008~0.010，且早期年龄和部分最新 fold 不稳定，因此不足以抵消数据源漂移风险。

### 早期年龄

| Age | CORE PR-AUC | CORE_PROMO | ENRICHED_NO_PRICE | ENRICHED_ALL |
|---|---:|---:|---:|---:|
| Day 7 | 0.275018 | 0.290970 | 0.251588 | 0.256770 |
| Day 14 | 0.296214 | 0.312029 | 0.310477 | 0.317277 |
| Day 30 | **0.438002** | 0.411657 | 0.440056 | 0.416795 |

Day30 是采购决策的重要窗口，CORE 表现稳健；广告/价格增强没有形成一致改善。

## 5. 特征稳定性风险

历史审计显示：

- 2024H1 广告相关字段虽然结构上非空，但正值率几乎为 0；后续时间块突然大量出现。
- `avg_price_7d/30d` 在 2024H1 整块缺失，2024H2 覆盖很低，到 2025H1 后接近 100%。

因此首版若使用这些字段，模型可能把“数据源上线年代”误学成业务信号。

## 6. 当前模型地位

```text
NV-RULE-V0
= 可解释规则雷达，保留用于实时辅助和对照

NV-ML-V1-A Logistic
= baseline

NV-ML-V1-B-CORE LightGBM
= 当前领先 challenger
= 尚未生产 Champion
```

## 7. 下一步

1. 基于 CORE 重新生成 strict temporal OOS 预测。
2. 分 Day7 / Day14 / Day30 / Day60 / Day90 做 score calibration 和 threshold policy 审计。
3. 先把模型输出视为 ranking score，不直接解释为真实概率。
4. 研究 HIGH / MEDIUM / LOW 是否需要按 age 分层阈值。
5. 阈值策略冻结后，再进入 all-daily-snapshot / first-alert / operational shadow 验证。
6. 最终仍需验证采购价值，且不得在未通过 gate 前替换生产 V4 / A16。
