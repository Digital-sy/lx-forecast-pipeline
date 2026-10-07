# 销量预测模型命名与术语指南

> 适用仓库：`Digital-sy/lx-forecast-pipeline`  
> 最后更新：2026-10-07  
> 目的：统一解释 A 系列、V 系列、H 系列，以及 HIGH / MEDIUM / LOW、Stage 1 / Stage 2、Champion / Challenger 等术语，避免把“模型编号”“系统版本”“预测月份”“预警等级”混为一谈。

---

## 1. 一页速查

| 名称 | 属于什么 | 核心含义 | 示例 |
|---|---|---|---|
| A0 / A3 / A16 / A31 | A 系列 | 月度销量预测研究中的算法/候选模型编号 | A16 是当前 ESTABLISHED 研究 Champion |
| V0 / V1 | Breakout 模型版本 | NEW_VISIBLE 爆发识别模型的版本 | V0=规则 baseline，V1=计划中的 ML 模型 |
| production V4 | 生产预测版本 | 当前仍在生产链路运行的旧版月度预测 | 与 Breakout V0/V1 不是一条版本链 |
| daily_monitor_v2~v6 | 监控实现版本 | 每日 Shadow Monitor 的工程实现修订 | v6 已加入 LCS/XH 业务排除 |
| H0 / H1 / H2 / H3 | Horizon | 预测目标距离当前预测月有几个月 | H0=当前月，H3=未来第3个月 |
| HIGH / MEDIUM / LOW | 预警等级 | Breakout 监控的加速强度等级 | V0 分数 >=70 为 HIGH |
| Stage 1 | 两阶段模型第一层 | 判断未来会不会 Breakout / 持续放量 | 输出概率 |
| Stage 2 | 两阶段模型第二层 | 条件于 Breakout 后预测销量体量 | 输出 future volume |
| Champion | 当前最佳已验证方案 | 暂时守擂的模型 | ESTABLISHED 当前研究 Champion=A16 |
| Challenger | 挑战 Champion 的新模型 | 必须经 temporal OOS 验证才能替换 Champion | A24/A25 等 |

最重要的区分：

```text
A 系列 = 算法/实验候选编号
V 系列 = 版本编号，但当前仓库存在多个 V 上下文，必须写清楚前缀
H 系列 = 预测时距，不是模型版本
HIGH/MEDIUM/LOW = 预警等级，不是销量预测月份
```

---

## 2. 整体预测架构

当前目标架构可概括为：

```text
                         ┌─────────────────────┐
                         │   SPU 生命周期路由   │
                         └──────────┬──────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          │                         │                         │
          ▼                         ▼                         ▼
   ESTABLISHED                NEW_VISIBLE              COLD_NO_HISTORY
      成熟款                    可观察新品                 冷启动新品
          │                         │                         │
          ▼                         ▼                         ▼
     A16 Champion             Breakout V0/V1           产品计划/属性模型
          │                         │                         │
          └──────────────┬──────────┴──────────┬──────────────┘
                         ▼                     ▼
                  H0/H1/H2/H3 预测       采购 / 成衣 / 面料
```

注意：当前生产采购链仍是 legacy V4；A16 是研究层冻结的 ESTABLISHED Champion，尚不能因为“研究 Champion”就直接等同于“生产已切换”。

---

# 3. A 系列：月度预测算法/实验候选

## 3.1 A 系列到底是什么

A 系列是销量预测研究过程中的算法实验编号。

它不是：

- 不是软件 release 版本；
- 不是 H0/H1/H2/H3；
- 不是 NEW_VISIBLE 的 HIGH/MEDIUM/LOW；
- 也不是按编号越大就一定越好。

A 编号主要用于记录：

> 在同一套 temporal OOS 回测框架下，不同基线、季节修正、类目门控、动量、Launch Curve、P75、Router 等策略的演进。

中间某些 A 编号只是诊断或失败实验，因此不要求 A0~A32 每个编号都进入正式模型。

---

## 3.2 关键 A 系列时间线

以下只列对当前系统理解最重要的节点，不追求穷举所有实验编号。

| 模型 | 仓库名称/概念 | 主要作用 | 当前理解/状态 |
|---|---|---|---|
| **A0** | `A0_上月延续` | 最简单基准：上月销量延续 | Baseline，用来判断复杂模型是否真的有增益 |
| **A1** | `A1_V4去floor` | 对 legacy V4 的 floor 逻辑做拆解实验 | 研究分支 |
| **A2** | `A2_SPU直接` | SPU 层直接预测基线 | 早期研究分支 |
| **A3** | `A3_SPU生命周期收缩` | 对 SPU 生命周期做收缩/衰减处理 | 重要 baseline；曾作为 NEW_VISIBLE safe baseline |
| **A5** | `A5_季节融合` | 将季节性信号融合到预测 | 研究分支 |
| **A7** | `A7_非对称季节门控` | 上升与下降季节信号使用非对称门控 | A16 的重要上游组成 |
| **A14** | `A14_SPU异常历史否决` | 对异常 SPU 历史进行 veto | 研究分支 |
| **A15** | `A15_稳健类目广度否决` | 当类目上涨证据缺乏广度时否决 uplift | A16 的前置思路 |
| **A16** | `A16_广度否决_高集中半衰减` | 高集中情况下不完全取消上涨，而是对 A7 uplift 做半衰减 | **当前 ESTABLISHED 研究 Champion** |
| **A17** | `A17_Amazon市场刹车` | 加入 Amazon 市场信号做下行刹车 | Challenger，未替代 A16 |
| **A18** | `A18_Amazon市场仲裁` | 市场信号仲裁实验 | Challenger/研究分支 |
| **A19** | `A19_最新月尖峰重锚` | 对最近月尖峰重新锚定 | 研究基线轨迹分支 |
| **A20** | `A20_严格衰退外推` | 对明确下行趋势做严格外推 | 研究分支 |
| **A21** | `A21_尖峰加严格衰退` | A19 + A20 组合 | 研究分支 |
| **A22** | `A22_再加弱动量` | 在前述轨迹基础上增加弱动量 | 研究分支 |
| **A24** | `A24_分规则时序校准` | 按规则做 temporal calibration | Challenger，未通过 promotion gate |
| **A25** | `A25_仅弱动量时序校准` | 只对弱动量做 temporal calibration | Challenger，未通过 promotion gate |
| **A26** | `A26_类目新品曲线` | 用类目历史新品 Launch Curve 修正 NEW_VISIBLE | Round19 结果偏向进一步低估，不采用 |
| **A27** | `A27_新品曲线半收缩` | Launch Curve 与 A3 半收缩 | 未解决 NEW_VISIBLE 严重低估 |
| **A28** | `A28_全局新品曲线` | 全局新品曲线 | 未成为正式方案 |
| **A29** | `A29_历史新品P75` | 使用历史新品 P75 轨迹提升 forecast | 能抬高预测，但容易伤害稳定/衰退新品 |
| **A30** | `A30_A3与P75取高` | `max(A3, A29)` 的 upward-only 方案 | 为 temporal router 提供候选 uplift |
| **A31** | `A31_全局时序P75强度` | 每个 horizon 用 prior OOS 月份学习 P75 blend 强度 | 有改善，但旧实验存在 temporal-label leakage 风险，**不可视为已验证生产模型** |
| **A32** | `A32_动量时序P75强度` | 按 snapshot 前动量 bucket 学习时序 P75 强度 | Research-only |

---

## 3.3 A16 为什么重要

A16 的核心不是“无脑加季节性”。

其思路是：

```text
A3 基线
  ↓
A7 非对称季节门控
  ↓
检查类目上涨是不是有足够广度
  ↓
若上涨高度集中：不把 uplift 全部放大
  ↓
只保留部分 uplift（半衰减）
```

因此 A16 更像一个“有证据才放量”的成熟款模型。

当前项目决策：

```text
ESTABLISHED Champion = A16
```

但生产切换仍需满足 temporal OOS、WAPE、Bias、ABC-A、滚动月份稳定性和采购影响等上线门槛。

---

## 3.4 A 系列不要怎么理解

错误理解：

```text
A31 > A16，因为31比16大
```

正确理解：

```text
A31 是后续实验编号；
是否更好必须看相同 temporal OOS 条件下 H0/H1/H2/H3 的 WAPE、Bias 和稳定性。
```

同理，A24/A25 编号更高，但没有通过 promotion gate，就不能替代 A16。

---

# 4. V 系列：版本编号，但当前有三个不同上下文

这是目前最容易混乱的部分。

仓库里“V”并不是一条单一版本线。

## 4.1 Breakout V0 / V1：NEW_VISIBLE 模型版本

### V0 = Rule V0

当前 NEW_VISIBLE Breakout 的规则 baseline。

主要看：

```text
近7天销量增长
近7天 Sessions 增长
CVR 变化
近7天绝对销量
（实时场景还可使用库存 DoS）
```

V0 输出：

```text
breakout_score
risk_level = HIGH / MEDIUM / LOW
reason_code
breakout_probability = NULL
```

为什么 probability 为空：

> V0 是规则打分，不是经过 OOS 校准后的统计概率，不能把 75 分假装成“75%爆发概率”。

当前历史回测说明：V0 有排序信号，但更像“新品加速探测器”，还不是最终的大爆款判定器。

### V1 = 计划中的机器学习版 Breakout

建议架构：

```text
Stage 1
P(Breakout / Persistence)
        ↓
Stage 2
Conditional Future Volume
```

V1 只有在严格 temporal OOS 下验证后，才允许真正输出 probability。

---

## 4.2 daily_monitor_v2 ~ daily_monitor_v6：工程实现版本

这是一条**每日 Shadow Monitoring 代码实现版本线**，不是 Breakout 模型本身。

例如：

```text
daily_monitor.py
daily_monitor_v2.py
...
daily_monitor_v6.py
```

当前 v6 的重要业务变化之一：

```text
LCS-* MSKU → 对应 SPU 整体排除
XH* SPU → 整体排除
```

并且：

- 原始 feature snapshot 可以保留事实数据；
- 被排除 SPU 不进入正常 Breakout scoring/alert；
- 不影响 production V4 采购预测链。

因此：

```text
MON-V6 ≠ Breakout V6
```

它只是 Monitor implementation version 6。

---

## 4.3 Production V4：生产月度预测旧版本

当前 production procurement pipeline 仍在使用 legacy V4 体系。

这和：

```text
Breakout V0/V1
```

完全不是同一条版本线。

建议以后讨论时明确说：

```text
PROD-V4      = 当前生产月度预测
NV-RULE-V0   = NEW_VISIBLE Breakout 规则版
NV-ML-V1     = 未来 NEW_VISIBLE 机器学习版
MON-V6       = 每日监控工程实现版本6
```

这样可以彻底消除“V4、V6、V0 到底谁更新”的歧义。

---

# 5. H 系列：Forecast Horizon

H 不是模型版本。

H = Horizon，表示预测目标距离当前预测月有多远。

假设预测截点在 2026-10：

| Horizon | 含义 | 示例 |
|---|---|---|
| **H0** | 当前月 | 2026-10 |
| **H1** | 下1个月 | 2026-11 |
| **H2** | 下2个月 | 2026-12 |
| **H3** | 下3个月 | 2027-01 |

例：

```text
ZSYxxx
H0 = 2,000
H1 = 3,500
H2 = 4,200
H3 = 3,000
```

意思是：

```text
当前月预计 2,000
下月预计   3,500
下下月预计 4,200
未来第3月  3,000
```

---

## 5.1 为什么强调 Direct H0/H1/H2/H3

目标架构不使用：

```text
H0 → H1 → H2 → H3
```

这种递归式预测。

原因是误差会逐层传递。

推荐：

```text
同一个 snapshot
├─ 直接预测 H0
├─ 直接预测 H1
├─ 直接预测 H2
└─ 直接预测 H3
```

即 Direct Multi-Horizon Forecasting。

尤其采购和面料决策更依赖 H2/H3，因此不能只优化近期 H0。

---

# 6. HIGH / MEDIUM / LOW：不是 H 系列

当前 V0 Breakout 规则：

| 等级 | 分数 | 中文解释 |
|---|---:|---|
| **HIGH** | >=70 | 强加速信号，值得重点关注 |
| **MEDIUM** | 45~69 | 有增长迹象，需要继续观察 |
| **LOW** | <45 | 当前没有强加速信号 |

HIGH 不等于：

- 一定会成为大爆款；
- H3 很高；
- 采购一定要立刻重仓。

HIGH 更准确的理解是：

> 当前销售/流量/CVR 等信号显示这个 NEW_VISIBLE SPU 正在明显加速。

最终是否值得大规模追单，需要 V1 的 Breakout probability、Persistence probability、未来销量体量和库存/采购约束共同判断。

---

# 7. Stage 1 / Stage 2

## Stage 1：会不会爆

目标是输出类似：

```text
P(Breakout) = 0.82
P(Persistence) = 0.71
```

Stage 1 是分类/概率问题。

## Stage 2：如果爆，会卖多少

输出可以是：

```text
future30 = 1,800
future60 = 3,400
future90 = 4,600
```

Stage 2 是条件销量预测问题。

注意：30天 Breakout 模型和月度 H0/H1/H2/H3 不是完全同一个 target。后续 H2/H3 仍应建立直接目标，不能拿 future30 递归滚到未来三个月。

---

# 8. 生命周期术语

| 名称 | 定义 | 典型模型 |
|---|---|---|
| **ESTABLISHED** | 已有较稳定历史的成熟 SPU | A16 |
| **NEW_VISIBLE** | 已开始真实销售，但历史仍短的新品 | Breakout V0/V1 + conditional volume |
| **COLD_NO_HISTORY** | 没有真实销售历史的未来新品 | 企划/属性/相似款/首单模型 |

当前 2026-10 研究方向：

```text
ESTABLISHED → A16
NEW_VISIBLE → 历史 snapshot + V0 baseline → V1 ML
COLD_NO_HISTORY → 等待产品计划/首单/属性输入完善
```

---

# 9. Champion / Challenger / Baseline

## Baseline

最低比较基准。

例如 A0“上月延续”。

一个复杂模型如果连 A0 都打不过，就没有复杂化的价值。

## Champion

当前相同验证体系下最可靠的方案。

当前：

```text
ESTABLISHED research Champion = A16
```

## Challenger

试图替代 Champion 的新方案。

例如 A24、A25、A31 等都属于不同阶段的 challenger/research candidates。

Challenger 只有满足以下 gate 才能 promote：

- Temporal OOS；
- H0-H3 WAPE 改善；
- Bias 可控；
- ABC-A 不恶化；
- 滚动月份稳定；
- 采购影响合理；
- 无时间泄漏。

---

# 10. Temporal OOS / Leakage

## Temporal OOS

严格按照时间训练和测试：

```text
较早月份 → Train
中间月份 → Validation
更晚月份 → Test
```

不能随机拆 daily snapshot。

同一个 launch 的连续 snapshot 高度相关，如果 Day10 在训练、Day11 在测试，会制造虚假的高分。

## Leakage

模型在预测时看到了未来才会知道的信息。

例如：

```text
snapshot_date = 2025-05-10
```

Feature 只能使用：

```text
<= 2025-05-10
```

future label 才允许使用：

```text
2025-05-11 以后
```

A31 旧实验必须特别谨慎，原因正是 temporal-label maturity / leakage 风险没有完全满足当前标准，因此不能把旧结果直接当成可上线结论。

---

# 11. WAPE / Bias

## WAPE

```text
WAPE = Σ|Forecast - Actual| / ΣActual
```

越低越好。

它回答：

> 总体预测绝对误差占真实销量多大比例？

## Bias

反映整体高估/低估：

```text
Bias > 0 → 总体买多风险
Bias < 0 → 总体买少/断货风险
```

采购系统不能只追求低 WAPE，还需要控制 Bias。

当前 promotion 目标通常要求 Bias 尽量靠近 0，研究 gate 约以 ±5% 为重要参考区间。

---

# 12. 推荐统一命名规范

从本文件开始，建议所有新文档、日志、报告采用如下写法：

| 推荐缩写 | 代表 |
|---|---|
| `A16` | 月度 forecast algorithm A16 |
| `PROD-V4` | 当前生产月度预测版本 |
| `NV-RULE-V0` | NEW_VISIBLE Breakout Rule V0 |
| `NV-ML-V1` | 未来 NEW_VISIBLE ML V1 |
| `MON-V6` | Daily Monitor implementation v6 |
| `H0~H3` | 月度 forecast horizons |
| `RISK-HIGH/MEDIUM/LOW` | Breakout monitor risk level |

示例：

```text
错误：
“V6 比 V4 好，所以换 V6。”

正确：
“MON-V6 是每日监控实现版本；PROD-V4 是生产月度预测，两者不是同一模型链。”
```

```text
错误：
“A31 是 H3 模型。”

正确：
“A31 是一个算法候选；它可以分别输出/评估 H0、H1、H2、H3。”
```

```text
错误：
“HIGH 就是 H3 高。”

正确：
“HIGH 是 NEW_VISIBLE 当前加速预警等级；H3 是未来第三个月销量预测。”
```

---

# 13. 当前状态快照（2026-10-07）

```text
生产链：
PROD-V4 继续运行，不因研究实验直接修改

ESTABLISHED：
A16 = frozen research Champion

NEW_VISIBLE：
732 个严格 launch cohort
80,774 条 point-in-time snapshots
NV-RULE-V0 = baseline / 加速探测器
NV-ML-V1 = 下一阶段目标

业务排除：
LCS-* MSKU 对应 SPU 整体排除
XH* SPU 整体排除

Forecast Horizon：
H0 / H1 / H2 / H3 直接预测
禁止 recursive roll-forward 替代 direct horizon
```

---

# 14. 最简记忆版

```text
A = Algorithm / 实验模型编号
V = Version / 版本，但必须写清楚是 PROD、NV 还是 MON
H = Horizon / 未来第几个月

A16 = 哪个算法
NV-RULE-V0 = 哪一版新品爆发判断
MON-V6 = 哪一版每日监控程序
H2 = 预测未来第2个月
HIGH = 当前加速预警很强
```

如果只记一句话：

> **A 决定“怎么算”，V 决定“是哪一版系统/模型”，H 决定“算未来多远”，HIGH/MEDIUM/LOW 决定“当前新品加速信号有多强”。**
