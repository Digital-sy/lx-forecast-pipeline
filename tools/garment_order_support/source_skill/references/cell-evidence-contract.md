# 输入输出约定

## 最小任务输入

只要求当前任务实际需要的来源：目标SPU/待确认ID；下单附件或已核验索引；可用的订单状态；若查领星，则还需真实面料颜色目录及版本。缺少状态不阻止提取事实，但阻止认定有效默认采购。

不要硬编码某批次的14款面料、25颜色、月份或数量。需求期间和数量从用户指定清单读取。

## 只读单元格定位器

在发现真实布局后制作本轮plan，不能猜列名/位置。示例为合成布局，不是模板：

```json
{
  "sources": [
    {"source_id": "s1", "path": "sources/order.xlsx", "url": "原文件链接", "expected_sha256": "填写实际SHA256", "acquired_at": "真实获取时间"}
  ],
  "records": [
    {
      "evidence_id": "ev001", "source_id": "s1", "sheet": "订单", "record_type": "ORDER_LINE",
      "header_fields": {"order_id": "J3", "fabric_header": "E3", "order_date": "A3"},
      "fields": {"sku_family": "A31", "garment_color": "B31", "fabric_color": "C31", "material_note": "G31"}
    }
  ]
}
```

路径相对plan所在目录解析。`expected_sha256`必须来自实际文件，尚无时省略，不填伪造值。时间未知时省略或null，不以现在时间冒充旧获取时间。

```powershell
& '<已核实的Python>' '<skill-dir>/scripts/read_order_cells.py' --plan '<任务目录>/plan.json' --output-dir '<任务目录>/extracted/run_001'
```

对于旧OLE `.xls`，需真实已有 `xlrd`；缺少时报告依赖，或使用获准的独立环境。若依赖位于已核验独立目录，可加 `--dependency-dir '<目录>'`。不安装同名网络包假定兼容，不改变生产环境。

提取器拒绝覆盖输出，记录文件SHA和读取能力；输出`evidence.json`与`manifest.json`。它不提取/查看图片，不解读面料，不认定有效订单。不能把`images=NOT_REVIEWED`说成无图片。xlsx公式只读公式与缓存，不执行Excel重算；xls公式文本能力不足时保留限制。

## 最终业务清单与证据JSON

业务清单建议一项待确认身份对应一组明确方案，组成项另行列行且共用`demand_id/scheme_id`：

|字段组|必要内容|
|---|---|
|待确认身份|review_id、demand_id、shop及证据、spu、garment_sku原值、source_color_system/code/name|
|订单与范围|order_id、order_status及获取时间、record_type、订单日期及来源、颜色/变体/部位适用范围、有效性限制|
|材料方案|scheme_id、component_id、part、material_raw、fabric_identity候选、version、alias_evidence、processing/pattern|
|实物颜色|number_raw、number_query、physical_color_name_raw；明确号、缺号、冲突分别保存|
|领星候选|全部SKU、完整原商品名、源面料/号、目录版本及同步时间、candidate_status|
|证据|source_id/url、文件哈希及下载时间、sheet/cell/merged_anchor或image锚点、raw_value、图片实际查看范围|
|结果|evidence_status、conflicts、remaining_action、future_applicability、final_mapping（默认未确认）|
|数量（若有）|原需求期间及件数、唯一需求键；历史订单数量独立列名，不混成未来需求|

推荐分别标状态，不能用一个PASS替代：
- 证据：`ORDER_LINE_NUMBER` / `REFERENCE_NUMBER` / `SCHEME_ONLY` / `NO_EVIDENCE`。
- 资格：`VALID_IN_SNAPSHOT` / `EXCLUDED` / `UNKNOWN`，并记录快照时间。
- 一致性：`CONSISTENT` / `CONFLICT` / `UNKNOWN`。
- 目录：`NONE` / `ONE_CANDIDATE` / `MULTIPLE_CANDIDATES` / `MATERIAL_IDENTITY_PENDING`。
- 未来方案：`CONFIRMED_BY_EXPLICIT_EVIDENCE` / `PENDING`；只有真实明确确认才能用前者。

同一订单正文与参考页互相冲突时保留两份evidence记录；业务清单不能只展示一个看似已定稿的号。人工已有确认单独保留，不因候选检索重新置空。
