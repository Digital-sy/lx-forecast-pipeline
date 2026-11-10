# 输入与版本合同 V1

## build配置

JSON配置只包含本机路径和核验选项，不含认证信息。相对路径以配置文件目录解析。

```json
{
  "inventory": "sources/inventory_current.json",
  "downloads": "sources/downloads_current.jsonl",
  "file_audit": "intermediate/order_file_audit.jsonl",
  "order_lines": "intermediate/order_lines.jsonl",
  "enriched_evidence": "private/逐色核查完整证据.json",
  "manual_evidence": "private/逐色核查完整证据_人工回填版.json",
  "status_index": "private/order_status_index.json",
  "verify_raw_files": true
}
```

前四项必需，后面三个证据文件可省略。可选 `expected_sha256` 为输入角色→预期SHA的对象。配置和业务内容存任务目录，不提交Git。

- inventory：已封存目录清单，`root/items/complete/errors/as_of_utc`；每项以token识别，保留name/type/url/modified_time及目录路径。
- downloads：逐次下载日志，不能把失败重试次数算成文件数。成功版本SHA不同须明确选择版本后另建索引，不能默默取最后一次。
- file_audit：原件级解析状态与SHA，包含未识别页、数量/日期异常。
- order_lines：具有line_id/token/source_sha256及原文证据的提取行。保存重复标记、不按行求和作采购事实。
- enriched_evidence：已有逐色解释的orders与master。保留原解析、解释、候选，不能将候选升级为确认。
- manual_evidence：相同来源基础上独立的manual_resolution。确认范围、导入时间、真实确认时间未知和缺项均保留。
- status_index：订单状态与按SPU记录的店铺证据；独立保留获取时间。状态快照不是所有店铺归属已确认的证明。

## 输出层

逻辑文件包括 documents、order_rows、enriched_rows、confirmations 与 manifest。实际文件名及哈希以manifest为准。

文档来源身份与内容版本分开；提取记录保留完整raw字段及证据位置；人工确认与机器事实分层。查到某条说明，只证明来源记录存在，不自动决定当前方案。

索引完整性指输入档案按合同被处理；不是订单日期、数量、颜色含义或远端完整性均已通过。来源清单不完整、原件缺失/变动、映射冲突分别留状态。

## 增量计划

`plan_refresh.py`只读两个inventory，输出每文件所需动作和独立订单状态变化。不下载、不改索引、不推进游标。

同token且metadata相同但无两边内容SHA，仅为 `METADATA_UNCHANGED_CONTENT_UNVERIFIED`。这是待内容核验，不是所有文件都必须立即重下载；后续获取策略需依赖已验证的版本合同。

相同根、相同范围且完整的当前清单中缺少来源，只标来源缺失并保留历史。权限变化/部分分页不能当删除。解析版本变化与源内容变化分别记录。

## 建表前待确认

本版不执行DDL。完成字段与真实样本验收后，才决定长期存储位置、业务主键、版本/人工确认引用与任务运行记录。任何最终表结构都不得把文件行号当作跨版本稳定身份，或把候选字段和人工确认合并覆盖。
