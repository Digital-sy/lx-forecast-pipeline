# DIRECT_CONTEXT90：受控离线复核

本目录只包含脱敏源码、测试和协议。不包含业务输入、模型、名单或逐款结果。
旧档案只读，运行结果必须放在仓库外的新目录；同名目录拒绝覆盖。

## 固定来源

- 父实现及诊断提交：`8908e71bc45bc0891b84fca5c4103058fc5ad984`。
- 历史回放参考：`34bdc3b703c45326f3c638814ad18595c8124e83`。
- 首次事前协议：`009144ff4eea1f0d14ed20b5c92704d77eefe0be`。
- 特征与缺失处理冻结：`6e30994c166c3bdc58bad89578ff1222c0a4dd73`。
- 效果计算前评分与独立复核代码：`202b4c25d87f3f5ac1804e4cb6438ce942794653`。
- 六个新增字段之外，原 DIRECT 的训练键、标签、旧特征、权重、
  资格、随机种子及参数均按归档逐期核对。
- 运行环境继承已归档的依赖锁；不更新生产环境。

## 优先复核已有结果，不重新训练

在独立环境、代码仓库根目录运行；下面路径为占位符，替换为已有受控归档。

```text
python -B scripts/direct_context90_validate.py
  --out <已完成的新实验目录>
  --reviewed <该目录/scoring>
  --destination <不存在的独立复核目录>
```

此操作从已有模型推断检查、独立重算日真值及评分，不调用模型拟合，不查询数据库。
复核所需包为现有冻结环境中的 pandas、numpy、lightgbm、pyarrow。

重新生成评分也不训练：

```text
python -B scripts/direct_context90_score.py
  --out <已完成的新实验目录>
  --diagnostic <已有交叉诊断/reviewed>
  --destination <不存在的新评分目录>
```

## 完整计算流程记录（非自动重跑指令）

仅在另有明确计算授权和新的输出目录时使用，不能通过换目录规避本轮 40 次预算。

```text
python -B tests/test_direct_context90.py --receipt <新的合成测试回执.json>
python -B tests/test_direct_context90_score.py
python -B scripts/direct_context90_experiment.py prepare
  --archive <冻结逐月回放目录>
  --out <新的实验目录>
  --synthetic-receipt <合成测试回执.json>
python -B scripts/direct_context90_experiment.py fit --out <新的实验目录>
```

先完成全部训练审计再拟合。`prepare` 不拟合业务模型。`fit` 每个 M1/M2 单元
仅允许一次；已开始但没有检查点的单元停止，不自动重训；有完整检查点时仅恢复预测。
模型检查点、正式拟合账本和哈希清单保存在业务归档，不能提交 Git。

## 范围与解释

这是已多次研究的重建历史开发验证，不是独立 OOS 或前瞻验收。
源记录的获取时间与业务时间不同；来源点时可获取性不因数值复算而获得证明。
当前品类、季节只参与事后分层，不进入训练。
计数单位为“店铺 × SPU × 签发期”，跨期可能为同一商品，目标窗口可重叠。

技术通过、五项数值筛选、业务可用性分别评价。不以单项改善替代全部门槛，
不以平均 WAPE 替代全体核心款三个窗口逐款 ±20% 的目标。

本工具无数据库写入、生产入口、部署、调度或模型切换能力。
