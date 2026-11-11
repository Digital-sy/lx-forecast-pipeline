# 成衣下单表材料颜色证据离线核查工具

本目录只包含通用源码、合成测试和使用说明，不包含业务数据、文件令牌、真实 SKU、凭据、结果明细或执行回执。

## 用途与边界

- `candidate_audit.py`：从已经封存的数据库回读、待确认清单及人工归档中整理候选证据。相同款号和颜色线索仅用于候选检索，不直接变成正式映射。
- `reference_followup.py`：核对人工事件的参考页、文件版本、SHA256 和单元格来源，保留缺证据及部分证据状态。
- 两者均只读输入文件并写入指定的本地输出目录，不连接数据库、不调用远程服务、不写回飞书、不修改人工决定。
- 参考页引用不能伪造为订单行证据；未知材料、颜色体系、实物色号或未来适用性不得自动补齐。
- 本工具沿用既有归档合同。部分输出文件名和展示列名保留历史命名，不代表程序硬编码业务数量；实际结果由输入文件计算。输入月份变化时应先审核合同，不直接用于新的业务批次。
- `reference_followup.py` 拒绝覆盖已存在的同名结果。运行任一工具时均使用新的输出目录。

## 运行环境

Python 3.11 或更新版本，使用 Python 标准库。脚本有 `__main__` 保护，导入不会执行业务核查。没有安装包或环境变量认证要求。

## 合成测试

在本目录执行：

```text
python -B -m unittest discover -s . -p "*test.py" -v
```

测试只使用合成对象和内存数据，不需要业务输入、不访问生产资源。测试覆盖精确款号边界、同色名末尾“色”的有限处理、颜色冲突保留、来源字段范围、图片证据、严格文件 token 与内容哈希绑定、版本冲突、未知状态和不伪造订单行 ID。

## 候选证据核查

先建立新的 `run/outputs` 与 `run/verification` 目录，再使用授权私有输入：

```text
python -B candidate_audit.py --output-root run --receipt private/production_readback.json --receipt-completion private/readback_completion.json --pending private/pending.csv --scope-summary private/scope_summary.json --manual private/manual_archive.json --new-raw private/new_raw_rows.jsonl --special-review private/special_review.csv --special-review-spu STYLE_SELECTED_FOR_EXISTING_REVIEW
```

可选参数：`--image-review private/image_review.json`，仅用于已有定点图像核查结果，不会自动识别或下载图片。

参数约定：

- `receipt` 为已有成功回读的 `tables` 包，含原始订单行、材料解释、文档版本及观察记录。
- `receipt-completion` 的 `production_receipt_sha256` 必须与回读文件一致。
- `pending` 保留原需求业务键与逐行数量；工具不使用历史采购行数量代替需求。
- `manual` 使用已有 `master` 及原人工决定；`new-raw` 是本次既有增量行，不触发新的获取。
- `special-review` 和可选 `image-review` 均为已经审核、精确指向既有业务键或原始行的输入，不是新业务授权。

## 人工确认引用核查

```text
python -B reference_followup.py --receipt private/production_readback.json --completion private/readback_completion.json --manual-archive private/manual_archive.json --review-registry private/manual_review_registry.json --builder private/original_import_builder.py --pending private/pending.csv --out reference_run
```

- `review-registry` 包含原人工判断和候选文件登记，用于核对支持文件编号。
- `builder` 只作为已有迁移实现的版本证据进行哈希归档；不会被导入或执行。
- 原文件路径来自授权归档。程序验证原文件内容 SHA256 与已保存的单元格证据关系，不通过同 SPU、同颜色名或单独色号猜测来源。
- 保留人工确认 ID、review ID、既有引用 ID、来源位置及诊断候选 ID；候选 ID 不是数据库正式引用。
- 完整候选、混合候选和无证据分别报告。没有明确实物色号的参考行不会被当作色号证据。

## 数据排除与交付

只提交本目录的四个 Python 文件和本说明。以下内容必须留在获准私有位置：

- 所有 JSON/JSONL/CSV/Excel 业务输入、数据库回读及清单；
- 文件 token、真实原文件链接、人工确认表和用户填写内容；
- 原始文件、图片、下载、日志、密钥及认证配置；
- 候选明细、数量汇总、报告、验证结果和运行目录。

使用结果进行数据库或飞书修改需经过独立审核与相应授权。本工具不会据候选自动确认材料、部位、色号或将历史证据升级为未来生产方案。
