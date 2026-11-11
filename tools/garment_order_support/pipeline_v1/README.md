# 下单表增量编排与固定月份需求刷新

本模块新增可重复的手动分阶段入口，不启用任何调度。真实文件、清单、业务结果、连接包装器和凭据不随代码发布。

## 来源入口

`source_pipeline.py` 默认 CHECK_ONLY，只验证配置及固定哈希。DISCOVER 显式只读列目录，PREPARE 只读取计划内新增/修订文件、使用已审核解析器并构建增量包。每阶段必须使用新目录，拒绝覆盖。

配置模板是占位合同，不可直接执行。工具、完整目录基线、上一已提交回执、DDL和解析依赖必须以实际获准文件及哈希配置。布局专用解析器仍是受控依赖；本模块不是任意表格的通用解析器，也没有公开的生产连接入口。

- 元数据未变不等于字节已核验。
- 来源变化列出受影响的人工确认引用，保留原确认，不自动换绑。
- 材料、部位、文字实物号仅提取有明确单元格证据的候选；图片、自由文字和冲突继续待核。
- 当前入库包只包含原始证据，不自动写入材料解释、人工确认或正式颜色映射。
- PREPARE完成不等于数据库提交。私有发布器必须独立校验包、当前基线和隔离回执，事务追加并独立回读。
- 只有实际COMMITTED_VERIFIED后，才可以将完整目录作为下一次比较基线；不得使用只含变更文件的子集代替完整目录。

```text
python -B scripts/source_pipeline.py --config /approved/config.json --output-dir /approved/new/check
python -B scripts/source_pipeline.py --config /approved/config.json --output-dir /approved/new/discover --stage DISCOVER --execute-read
python -B scripts/source_pipeline.py --config /approved/config.json --output-dir /approved/new/prepare --stage PREPARE --execute-read --discovery-run /approved/new/discover
```

## 需求刷新

`refresh_fabric14_demand.py`是本地只读计算；本版固定2026-11和2026-12字段，不宣称已实现跨月自动适配。公开副本只将私有默认路径改成必填参数，算法不变。

严格以店铺、SPU、颜色体系和颜色缩写关联；数值id仅追溯。保留人工确认的原始适用范围、未确定状态和明确零。新正需求没有该颜色材料方案时单列，不借用同款另一颜色。

原净建议下单量不重复扣库存。输出单位为成衣件，不是面料米数、条数或采购批准数量。材料参与量与去重需求量分别统计。源业务时间与读取时间分开。

```text
python -B scripts/refresh_fabric14_demand.py --current /approved/current.json --old /approved/old.json --base-dir /approved/accepted-mapping --full-conversion /approved/material-conversion.json --output /approved/new/refresh
python -B -m unittest discover -s tests -v
```

旧输入及结果不可覆盖。测试使用合成数据和临时目录；不调用飞书、数据库或预测模型。生产wrapper、业务工作簿、原始数据、回读内容与连接配置均保留私有。本提交不合并main、不部署。
