# 调用与验证

所有命令先用 `--help` 核对本版参数。Python索引和规划器仅用标准库；单元格读取需要已有openpyxl，旧xls需要已有xlrd。不要导入生产入口或安装同名SDK替代。

## 顺序

1. 从明确根目录生成当前文件清单（不下载附件）。没有当前需求时直接用历史清单，但明确日期。
2. 比较旧新inventory生成refresh plan；原件版本变化与订单状态变化分别处理。
3. 在独立目录取得所需新增/修订附件。这个获取执行步骤复用已有文件技能，本工具不自动执行。
4. 复用现有解析结果或经明确布局提取新证据，再build新索引，旧索引保留。
5. query定位后，交业务取证技能逐色判断；保留人工确认。

## 示例命令（替换实际路径）

```powershell
& '<python>' '<skill>/scripts/discover_order_files.py' --help
& '<python>' '<skill>/scripts/plan_refresh.py' --previous '<旧inventory>' --current '<新inventory>' --previous-parser-version '<旧版本>' --parser-version '<新版本>' --output-dir '<新计划目录>'
& '<python>' '<skill>/scripts/order_index.py' build --config '<私有配置>' --output-dir '<新索引目录>'
& '<python>' '<skill>/scripts/order_index.py' query --index '<索引目录>' --spu '<款号>' --output-dir '<新查询目录>'
& '<python>' '<skill>/scripts/read_order_cells.py' --plan '<已核实布局计划>' --output-dir '<新提取目录>'
```

单元格计划细节见 [cell-evidence-contract.md](cell-evidence-contract.md)。这里只复制已固定的只读提取器；图片和语义判断仍未由它执行。

## 测试

在源码目录先设置本次进程的 `ORDER_SOURCE_TEST_ROOT` 和 `GARMENT_SOURCE_TEST_TMP` 为获准的任务临时目录，再执行 `python -B -m unittest discover -s tests -v`。不修改机器级TEMP/TMP。测试产生合成临时目录，使用任务专用临时路径，不连接线上。正式归档的输入哈希和真实查询另验，不把合成测试数量当线上成功。

每次发布技能记录源码SHA、版本、测试命令和实际回执。先看索引目录的 `manifest.json` 和查询目录的 `manifest.json`是否存在异常及截断，再引用结果。不得将被截断结果、文件名线索或未验证原件当完整事实。
