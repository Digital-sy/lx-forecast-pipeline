# A7 DOWN有限敏感性实验：复现与安全边界

计算前预登记：`cb4a0b723c7b2f04f50469c85a3394649464b5c2`。
先阅读同目录`a7_down_sensitivity_v1_preregistration.md`。此工具只接受已归档本地输入，不连接数据库，不运行生产预测或新影子任务，不写入原归档。

## 准备

使用Python标准库即可。输入清单为本地JSON，结构为`{"versions":{"monthly_and_seasons":{"path":"/authorized/archive/file"}, ...}}`；所有键及文件SHA256必须与已提交的`a7_down_sensitivity_v1_inputs.json`一致。业务文件由有权限的操作者本地提供，绝不提交Git。不要构造替代业务数据使校验通过。

## 验证模式：不计算敏感性效果

```sh
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -p 'test_a7_down_sensitivity_v1.py' -v
python scripts/a7_down_sensitivity_v1.py --inventory /authorized/source_inventory.json --validation-only --output /authorized/new_validation_directory
```

验证固定九个历史月份、H2/H3的冻结基线及完整校准上下文，并给原1000条增加UNKNOWN等可信度标签。此模式不计算w=0.5/0、不生成候选改善结论。历史基线验收不等于敏感性实验已完成。

## 业务门槛确认后才运行

必须先取得真实业务确认；本地执行凭据包含`gate_confirmed: true`、`prereg_commit`、带时区的实际`confirmed_at`和真实`user_confirmation`。不得把合成测试中使用的确认样例当作业务授权，不回填时间。若业务改变筛选规则，先追加预登记并修订验证，不在看过结果后修改门槛。

```sh
python scripts/a7_down_sensitivity_v1.py --inventory /authorized/source_inventory.json --approval /authorized/business_confirmation.json --output /authorized/new_sensitivity_directory
```

输出必须使用不存在的新目录。工具先验证全部归档指纹、冻结依赖和原校准训练范围。只改变DOWN混合权重，冻结A16后续DOWN保持规则必须成立，非DOWN不变。缺少改动所需输入时新候选输出NULL；原权重对照保留原值和缺失警示。停止条件详见预登记。

所有输出是重建历史诊断，不是独立OOS，不证明资料当时可获取。原未来批次尚无实销真值，标记验收不能解释为准确率改善。

## 产物

验证：`validation_summary.json`、`baseline_details.json/csv`、`contexts.json/csv`、`original_batch_tagged.json/csv`。

敏感性：`details.json/csv`、`metrics.json/csv`、`changes.json/csv`、`summary.json`及上下文和原批次诊断。完整母体、共同评分分母、未知真值、日源明确零和冲突真值分别保留。业务产物只存授权本地路径；本分支仅保存源码、合成测试和脱敏规范。
