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


## 8. Daily first-alert 运营回放结论

固定 checkpoint 阈值在 daily replay 下出现明显的 repeated-trigger / sequential-trigger 膨胀：

- MEDIUM/WATCH 首次报警过宽，告警率过高，只适合作为观察池；
- HIGH 明显优于 RULE V0，但 first-alert precision 仍不足以作为自动追单门槛；
- 2consec（连续两天越线）是当前最合理的固定去抖规则：比 1of1 提高 first-alert precision，且基本不损失 useful recall；
- 2of3 / 3of5 没有形成足够大的额外收益，不继续扩大确认规则搜索空间。

因此正式业务语义暂定：

```text
WATCH
= 值得运营/采购关注
= 不触发自动追单

Stage-1 HIGH / ACTION CANDIDATE
= 高价值人工复核池
= 不作为单独自动采购依据
```

## 9. Daily-event 阈值前向验证

Daily-event 阈值策略已加入严格 label maturity guard：

```text
history snapshot_date + 30 days < test_fold_start
```

原因：按 launch 所属半年分 fold 时，H1 launch 的 Day60/90/120 snapshot 可能自然延伸进 H2；仅按 launch fold 过滤不足以防止 future-label leakage。

修正后 P50/P55/P60 三种历史 precision target 仍未在未来 fold 稳定维持 50%+ first-alert precision。

当前结论：

- 不继续把 Stage-1 HIGH 优化成自动追单开关；
- Stage-1 的核心价值冻结为 ranking / opportunity screening；
- 采购动作必须进入 Stage-2 条件销量预测，并与库存、在途、补货周期组合。

## 10. Stage-2 研究方向

下一阶段模型：

```text
NV-ML-V1-STAGE2-COND30-CORE
```

目标：

```text
E(future_sales_30d | PERSIST_750)
```

首轮仅使用 CORE 特征，并采用同样的 strict temporal OOS / no-launch-overlap 规则。

同时诊断：

```text
expected_opportunity_units
= P(PERSIST_750) × E(future_sales_30d | PERSIST_750)
```

注意：这不是 NEW_VISIBLE 总销量预测；negative class 仍可能产生销量。后续若 Stage-2 条件销量有效，再补 negative/base-demand 组件和 P50/P75 quantile，最终形成可接库存/在途/供应链提前期的完整 future30 数量预测。


## 11. Stage-2 条件销量首轮结果

`NV-ML-V1-STAGE2-COND30-CORE` 在四个 temporal OOS fold 中均优于简单的同年龄正类中位数 baseline。

Pooled 条件销量：

| 指标 | Age-median baseline | LightGBM Stage-2 |
|---|---:|---:|
| Row WAPE | 0.4223 | **0.3375** |
| Row Bias | -0.1911 | -0.1953 |
| Launch-balanced WAPE | 0.3996 | **0.3072** |
| Launch-balanced Bias | -0.0657 | -0.1329 |
| Median APE | 0.2994 | **0.2129** |
| P75 APE | 0.5135 | **0.3640** |

按生命周期：

- Day7 WAPE：0.2368 → 0.2298
- Day14 WAPE：0.2643 → 0.2470
- Day30 WAPE：0.4283 → 0.3574
- Day60 WAPE：0.4629 → 0.3627
- Day90 WAPE：0.5265 → 0.3863

结论：

1. Stage-2 条件销量模型成立，尤其 Day30/60/90 改善明显。
2. 当前主要问题从“是否有预测能力”转成“系统性低估大赢家”：LightGBM 条件销量 Bias 仍偏负。
3. 因此下一步不继续优化单点回归，而进入 P50/P75 quantile。
4. `Stage1 probability × Stage2 conditional volume` 的 opportunity-units WAPE 仍 >1，不可直接解释为完整 future30 总销量；后续必须补 non-breakout/base-demand 组件。

## 12. Stage-2 Quantile 下一步

新增研究模型：

```text
NV-ML-V1-STAGE2-QUANTILE-COND30-CORE-Q50
NV-ML-V1-STAGE2-QUANTILE-COND30-CORE-Q75
```

目标：

- P50：条件销量中位数，偏向常规采购计划；
- P75：条件销量上分位，研究缺货风险/安全采购量；
- 严格检查 empirical coverage：P50 应接近 50%，P75 应接近 75%；
- 优先看 Day14 / Day30，因为最接近真实补货动作窗口；
- 同时检查 quantile crossing（P75 < P50）。

Quantile 通过后，再补 negative/non-breakout base-demand 组件，形成完整 future30 总销量分布。


## 13. Stage-2 Quantile 首轮结果

Q50 与上一轮 Stage-2 `regression_l1` 条件销量点模型本质一致，均表示条件中位数，因此后续不再维护两套独立 P50 模型。

### P50

Pooled：

- Row coverage = 0.4602
- Launch-balanced coverage = 0.5125
- Pinball loss 明显优于 age-P50 baseline
- Day14 coverage = 0.5190
- Day30 coverage = 0.5517

因此 P50 已基本具备“条件常规销量”语义，优先以 launch-balanced coverage 判断。

### P75

Pooled：

- Row coverage = 0.6458
- Launch-balanced coverage = 0.6969
- Pinball loss 显著优于 age-P75 baseline
- Day14 coverage = 0.6709
- Day30 coverage = 0.6897
- Day60 coverage = 0.4835
- Day90 coverage = 0.6914

结论：

1. Q75 的排序/损失表现优于简单 age-P75 baseline；
2. 但 empirical coverage 明显低于 75%，说明上尾仍被低估；
3. 因此 raw Q75 暂不能直接解释为“75%采购安全量”；
4. quantile crossing 较低且随时间改善（约 9.2% → 3.6% → 3.5% → 2.0%），可通过 `P75=max(P75,P50)` 单调后处理解决。

### 下一步

新增 forward-only quantile calibration：

```text
actual / raw_quantile prediction
→ prior mature OOS ratio quantile
→ multiplicative calibration factor
```

并严格要求：

```text
calibration label_end_date < test_fold_start
```

优先比较：

- RAW
- GLOBAL calibration
- AGE calibration（样本不足时回退 GLOBAL）

P75 校准目标：coverage 接近 75%，同时 pinball loss 不明显恶化。


## 14. Stage-2 Quantile 校准结论

Forward-only calibration 已通过 maturity guard，所有测试折都满足：

```text
calibration label_end_date < test_fold_start
```

### P50

RAW P50 保持最佳业务语义：

- pooled launch-balanced coverage ≈ 0.514
- Day7 / Day14 / Day30 coverage ≈ 0.490 / 0.527 / 0.557

GLOBAL / AGE 校准会把 P50 推高到约 0.55~0.57 coverage，没有必要。

因此：

```text
P50 = RAW
```

### P75

Pooled：

- RAW row coverage ≈ 0.664，launch-balanced ≈ 0.722
- GLOBAL row coverage ≈ 0.774，launch-balanced ≈ 0.824
- AGE row coverage ≈ 0.770，launch-balanced ≈ 0.818

但分年龄看：

- RAW Day7 / Day14 / Day30 launch-balanced coverage ≈ 0.735 / 0.727 / 0.738，已经接近 75%
- GLOBAL/AGE 把 Day14/30 推到约 0.87 / 0.82，明显过度保守
- 真正失真的主要是 Day60，RAW coverage 只有约 0.44；Day90 RAW 约 0.69

因此不采用统一 GLOBAL/AGE 校准层覆盖全部年龄。

当前冻结：

```text
P50:
RAW

P75:
Day7 / Day14 / Day30 = RAW
Day60 / Day90 = research-only，暂不用于早期采购安全量
```

这避免为了修复晚期 lifecycle 而破坏真正关键的 Day14/30 补货窗口。

## 15. Non-breakout / Base-demand 下一步

Stage-1×Positive-volume 不能代表完整 future30，因为 target=0 的新品仍然产生销量。

新增：

```text
NV-ML-V1-STAGE2-BASE30-CORE
NV-ML-V1-STAGE2-DIRECT30-CORE
NV-ML-V1-STAGE2-MIXTURE30-RAW-P
```

比较三条路线：

1. negative/base-demand conditional model；
2. two-component mixture：
   `p × positive_volume + (1-p) × base_volume`；
3. 直接对全部 NEW_VISIBLE 预测 future30 的 DIRECT LightGBM。

DIRECT 是必须保留的简单 benchmark。若 DIRECT 比 mixture 更稳、更准，则不为了架构整齐强制使用分类概率混合。

优先评估 Day14 / Day30 的 WAPE、Bias 和 temporal stability。


## 16. Base-demand / Full-volume 结果与架构选择

### 数据事实

PERSIST_750=0 并不等于“没销量”。

全样本：

- Negative rows：2872，future30 销量合计约 1,010,309
- Positive rows：585，future30 销量合计约 1,006,424
- Negative 销量占比 ≈ 50.1%
- Positive 销量占比 ≈ 49.9%

因此 Stage1×Positive-volume 只能解释约一半的真实销量，必须有 base-demand / full-volume 组件。

### Negative conditional model

`NV-ML-V1-STAGE2-BASE30-CORE` 明显优于 negative age-median：

- launch-balanced WAPE：0.7884 → **0.4529**
- Bias：-0.6334 → **-0.2393**

说明 non-breakout/base-demand 本身可预测，不应被视为纯噪声。

### Full future30：DIRECT vs MIXTURE

Pooled launch-balanced：

| Model | WAPE | Bias |
|---|---:|---:|
| DIRECT | **0.4607** | -0.2575 |
| MIXTURE | 0.4722 | **-0.2065** |

四个 temporal fold 中 DIRECT 的 WAPE 都低于 MIXTURE。

按生命周期：

- Day7：MIXTURE WAPE 0.5709，优于 DIRECT 0.6012；
- Day14：两者几乎相同，MIXTURE 0.4821 vs DIRECT 0.4847；
- Day30：DIRECT 0.4595，略优于 MIXTURE 0.4613；
- Day60 / Day90：DIRECT 明显更优。

因此当前主数量架构冻结为：

```text
Stage1:
NV-ML-V1-B-CORE
= opportunity ranking / explanation
= 不强制进入数量公式

Full future30 quantity:
NV-ML-V1-STAGE2-DIRECT30-CORE
= 当前完整销量主 challenger

Positive / Negative conditional models:
= 保留为诊断、解释和后续分层研究
```

不采用 MIXTURE 作为主数量模型，原因是它虽然 Bias 更小，但没有带来更低 WAPE，而且增加了对 Stage1 probability calibration 的依赖。

## 17. Full-volume Quantile 下一步

既然 DIRECT 被选为完整 future30 主架构，下一步直接在全部 NEW_VISIBLE 上训练：

```text
NV-ML-V1-STAGE2-DIRECT30-CORE-Q50
NV-ML-V1-STAGE2-DIRECT30-CORE-Q75
```

目标：

- Q50：完整 future30 常规需求线；
- Q75：完整 future30 安全采购线候选；
- 严格 temporal OOS；
- 优先检查 Day14 / Day30；
- 通过后才接库存、在途、生产20天 + 海运28天做采购回放。

Stage1 保留作为风险/机会解释层，而不再作为完整数量预测的必经公式。


## 18. DIRECT full-volume Quantile 首轮结果

完整 future30 主架构仍冻结为：

```text
NV-ML-V1-STAGE2-DIRECT30-CORE
```

但 raw full-volume quantile 的 coverage 还未达到业务语义要求。

### Q50

Pooled launch-balanced：

- coverage ≈ 0.433
- WAPE ≈ 0.461
- Bias ≈ -0.257

分年龄 coverage：

- Day7 ≈ 0.359
- Day14 ≈ 0.395
- Day30 ≈ 0.474
- Day60 ≈ 0.418
- Day90 ≈ 0.479

Q50 的 pinball loss 明显优于 age-Q50 baseline，但整体偏低估，尤其 Day7/14。

### Q75

Pooled launch-balanced：

- coverage ≈ 0.656
- pinball loss 明显优于 age-Q75 baseline
- aggregate Bias ≈ +0.049

分年龄 coverage：

- Day7 ≈ 0.611
- Day14 ≈ 0.617
- Day30 ≈ 0.685
- Day60 ≈ 0.607
- Day90 ≈ 0.701

因此 raw Q75 暂不能解释为完整 future30 的 75% 安全需求线。

Quantile crossing 很低（各 fold 约 0.2%~2.3%），后续继续使用：

```text
Q75 = max(Q75, Q50)
```

### 技术解释

DIRECT 当前点模型使用 `regression_l1`，本质上更接近条件中位数。偏态销量分布下，中位数预测的总量 Bias 为负并不等同于模型失败；采购决策应通过 P50 / P75 分布表达风险，而不是强制 P50 aggregate Bias=0。

## 19. DIRECT Quantile forward calibration

下一步不更换模型，只审计 forward-only multiplicative calibration：

```text
ratio = actual_future30 / raw_quantile
factor_q = prior mature OOS ratio 的 q 分位
calibrated = raw × factor_q
```

比较：

- RAW
- GLOBAL factor
- AGE factor（样本不足时 fallback GLOBAL）

严格要求：

```text
calibration label_end_date < test_fold_start
```

优先判断 Day14 / Day30：

- Q50 coverage 是否接近 50%
- Q75 coverage 是否接近 75%
- pinball loss 是否保持优势
- forward fold 是否稳定

若通过，则停止继续增加预测模型层，进入库存 / 在途 / 生产20天 + 海运28天的采购回放。


## 20. DIRECT30 Quantile forward calibration 结论

所有测试折均通过 maturity guard：

```text
calibration label_end_date < test_fold_start
```

### Q50

Pooled launch-balanced：

- RAW coverage ≈ 0.423
- GLOBAL coverage ≈ 0.490
- AGE coverage ≈ 0.491

Day14 / Day30：

- GLOBAL ≈ 0.457 / 0.549
- AGE ≈ 0.481 / 0.485

因此 Q50 的校准语义已经成立。若只看简单性与 pooled 指标，GLOBAL 略优；若优先 Day14/30 对称性，AGE 更贴近 50%。

### Q75

Pooled launch-balanced：

- RAW coverage ≈ 0.642
- GLOBAL coverage ≈ 0.761
- AGE coverage ≈ 0.751

Day14 / Day30：

- GLOBAL ≈ 0.719 / 0.823
- AGE ≈ 0.767 / 0.741

因此 Q75 更适合 AGE calibration：它在最重要的 Day14 / Day30 更接近真正 75% coverage；GLOBAL 在 Day30 明显偏保守。

当前 H30 研究冻结：

```text
DIRECT30 Q50:
GLOBAL 或 AGE 都可用；AGE更贴近Day14/30对称coverage

DIRECT30 Q75:
AGE calibration

Stage1:
继续只做 opportunity ranking / explanation
```

但 H30 不直接作为采购补货主周期，因为返单供应链约为 20 天生产 + 28 天海运 = 48 天。

## 21. 采购主周期切换到 DIRECT48

不采用：

```text
Future48 = Future30 × 48/30
```

因为这会引入未验证的线性销量假设。

新增独立 research-only 脚本：

```text
scripts/train_new_visible_v1_stage2_direct_h48.py
```

它：

- 不修改现有 snapshot 表；
- 复用冻结的 point-in-time CORE 特征；
- 从 `forecast_research_spu_daily_history` 直接计算：
  `future_sales_48d = snapshot+1 ... snapshot+48`；
- 只保留完整 48 天未来窗口；
- 使用 strict temporal OOS；
- 同时输出 RAW / GLOBAL / AGE 的 Q50 / Q75 forward calibration；
- 2026H2 因 48 天 maturity window 样本减少属于正确右截断。

H48 通过后，才进入 live inventory / inbound procurement shadow。由于 2026-09-30 前缺少可靠历史每日库存，不允许伪造历史库存做采购回测。


## 22. DIRECT48 strict temporal OOS 结果

H48 使用真实未来 48 天标签：

```text
future_sales_48d = snapshot+1 ... snapshot+48
```

现有 H30 snapshot 表未修改；H48 标签直接来自 `forecast_research_spu_daily_history`，并只保留完整 48 天未来窗口。

所有 forward calibration fold 的 maturity guard 均通过。

### RAW

H48 raw quantile 仍明显低估：

- Q50 pooled launch-balanced coverage ≈ 0.416
- Q75 pooled launch-balanced coverage ≈ 0.618

因此 raw H48 不作为采购语义输出。

### GLOBAL calibration

Pooled launch-balanced：

- Q50 coverage ≈ 0.492
- Q75 coverage ≈ 0.758
- Q50 WAPE ≈ 0.487

分年龄：

- Day14：Q50 ≈ 0.458，Q75 ≈ 0.735
- Day30：Q50 ≈ 0.540，Q75 ≈ 0.796

### AGE calibration

Pooled launch-balanced：

- Q50 coverage ≈ 0.494
- Q75 coverage ≈ 0.756
- Q50 WAPE ≈ 0.489

分年龄：

- Day14：Q50 ≈ 0.505，Q75 ≈ 0.772
- Day30：Q50 ≈ 0.496，Q75 ≈ 0.781

因此从 pooled / lifecycle coverage 看，AGE 更贴近 Q50/Q75 的业务语义；GLOBAL 的 pinball/WAPE 略好且更简单。

但 forward fold 内仍存在明显波动：

- 2025H2 AGE Day14 Q50 ≈ 0.402、Q75 ≈ 0.709；
- 2026H1 AGE Day14 Q50 ≈ 0.627、Q75 ≈ 0.822；
- 2026H2 AGE Day30 仅 39 行，Q50 ≈ 0.333、Q75 ≈ 0.590。

因此：

```text
DIRECT48
= 需求预测研究通过
= 可以进入 live procurement shadow
= 尚不能作为自动追单 Champion
```

Shadow 阶段建议同时保留 GLOBAL / AGE 两套校准输出，观察真实 Day14/30 稳定性后再冻结最终线上校准方式。

## 23. H48 procurement shadow 前置条件

当前 live `forecast_feature_snapshot_daily` 与冻结 CORE 特征并不完全一致。

训练 CORE 包含但 live feature snapshot 缺失的字段包括：

- sales_3d / sales_prev_3d
- sessions_3d / sessions_prev_3d
- cvr_3d
- sales_growth_3d / sessions_growth_3d
- sales_positive_days_7 / sessions_positive_days_7
- sales_up_days_7 / sessions_up_days_7
- sales_slope_7 / sessions_slope_7
- sales_cv_7 / sessions_cv_7
- sales_max_day_share_7 / sessions_max_day_share_7

因此禁止直接用缺失值替代后给 H48 live 打分。

新增只读审计：

```text
scripts/audit_new_visible_h48_shadow_readiness.py
```

它检查：

1. research daily history freshness；
2. 当前 NEW_VISIBLE 范围；
3. live feature schema 与冻结 CORE 的一致性；
4. 当前真实 FBA inventory snapshot 覆盖；
5. inbound shipped / receiving 等库存组件覆盖；
6. LCS/XH business exclusion；
7. 是否允许进入下一步 live CORE scorer。

只有完成 live CORE 同口径特征构造后，才进入 H48 Q50/Q75 + 库存/在途采购 shadow。


## 24. Live H48 shadow implementation

Readiness audit confirmed two blockers on 2026-10-06 live snapshots:

1. existing `forecast_feature_snapshot_daily` has only 18 of 38 frozen model inputs directly available and is missing the training CORE fields for 3-day windows, slope/CV/positive-days/up-days/max-day-share;
2. current NEW_VISIBLE FBA snapshot coverage is 85/100 shop×SPU, so 15 missing rows must be distinguished between true no-inventory and mapping/source gaps before any procurement quantity is allowed.

Therefore the implementation does not silently impute or treat missing inventory as zero.

### New live CORE table

```text
forecast_new_visible_core_snapshot_daily
```

Materializer:

```text
scripts/materialize_new_visible_live_core.py
```

Properties:

- scope starts from current NEW_VISIBLE lifecycle rows;
- applies LCS/XH business exclusions;
- exact first sale comes from the first positive daily sale;
- all frozen CORE formulas mirror historical snapshot construction;
- feature windows end at the freshest completed research-daily date;
- rows without an exact daily first sale are not scored;
- shadow-only, no production-table writes.

### Inventory gap audit

```text
scripts/audit_new_visible_inventory_gaps.py
```

For every current NEW_VISIBLE row absent from the FBA inventory snapshot, it checks:

- latest SKU→SPU product mapping;
- raw FBA source;
- fallback `FBA库存明细`;
- local `库存预估表`.

A missing inventory row is never automatically converted to zero. Mapping/snapshot failures remain blocked.

### H48 live scorer

```text
scripts/score_new_visible_h48_shadow.py
```

Output table:

```text
forecast_new_visible_h48_prediction_daily
```

The scorer:

- reads only the training-identical live CORE table;
- refits DIRECT48 Q50/Q75 using only mature historical H48 labels;
- excludes current live launch keys from training to preserve strict OOS-style discipline;
- rebuilds strict historical OOS calibration residuals;
- outputs RAW / GLOBAL / AGE-calibrated Q50/Q75;
- maps live ages to the validated lifecycle bands 7/14/30/60/90;
- age <7 or >120 remains watch-only;
- writes only the new shadow prediction table.

Production V4 and all current procurement tables remain untouched.
