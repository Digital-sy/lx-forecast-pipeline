# RAW_ONLY 真实增量构包

`build_increment_package.py` 读取已封存的文件获取、解析、索引产物和已核验的九表只读快照，构造不可覆盖的本地追加包。它不调用飞书、不连接数据库、不启用调度。

## 运行合同

1. 输入索引须为 `LOCAL_INDEX_COMPLETE`，文件字节全部核验；获取须为 `COMPLETE`，解析须为 `PARSED` 且逐文件 `ok`。`parsed_with_review`、`no_order_lines_review` 等状态保持原样并阻断本入口，不会改写为成功。该限制属于本构包入口，并不表示这些文件不可单独归档。
2. 配置 `schema_version` 固定为 `garment_raw_increment_package_config/1`；`paths` 明确提供 `index_dir`、`acquisition_manifest`、`fetch_plan`、`parse_manifest`、`downloads`、`fetch_allowlist`、`acquisition_inventory`、`parse_inventory`、`parse_actions`。索引元数据包装清单和解析原清单可不同，分别按各自 manifest 核验哈希。
3. `--production-before` 是受控调用方已封存的 `SNAPSHOT_VERIFIED` JSON 文件，包含 `metadata` 和完整九表 `tables`。本工具只读本地文件，不取得或扩大数据库权限。公开包不包含快照、下载、配置或业务数据。
4. 文件身份按已确认的源系统、资源类型及 source_id；版本按既有版本 ID 和内容哈希。既有 document/version 的全部业务字段原样复用；新下载路径、实际获取时间写入新 observation。自然键、主键、字节长度或格式冲突均拒绝，不任取记录。
5. 新增范围仅为获取批次、文件身份、内容版本、观察、提取和原始逐色证据。材料解释、人工确认、确认引用三表在包中始终为空；原行及自动时间由追加发布器保护。原始面料表头 `fabric_header` 映射到 `fabric_header_raw`，未知仍为 NULL，不做颜色或部位猜测。
6. 沿用既有 25,000 行预算、九表结构、字段约束和追加图校验。输入变动、重复键、外键/自然键冲突会在写包前阻断。包生成后通过既有 loader 独立回读文件哈希和行数。
7. 输出保持 `LOCAL_PACKAGE_VALIDATED_NOT_IMPORTED`。构包成功不是数据库已提交，也不是面料颜色映射已批准、有效采购订单或未来默认用料方案。生产发布必须使用受控调用入口和当前授权范围。

## 本地复现

在独立工作目录中为测试设置 `GARMENT_SOURCE_DDL` 指向本公开目录的 SQL，并把 `GARMENT_SOURCE_TEST_TMP` 指向已存在的独立临时目录。只对本进程设置变量，不改变系统 TEMP/TMP。

```text
python -B -m unittest discover -s tests -p test_build_increment_package.py -v
python -B scripts/build_increment_package.py --config CONFIG_JSON --production-before SNAPSHOT_JSON --ddl sql/lingxing_garment_order_schema_v1_1.sql --output NEW_PACKAGE_DIRECTORY
```

测试仅用合成文件与本地快照；不据此声称真实数据库集成已验收。实际数据库验证及业务回执保留在私有任务归档。

## 版本边界

公开适配器版本 `1.0.0-public1`，运行适配器版本 `1.0.0`。二者依赖哈希不同：公开版本固定现有脱敏 `build_load_package.py`、`mysql_load_core.py` 与 SQL 的字节；私有运行版本固定原运行工件。公开 SQL 与运行 SQL 的差别为既有脱敏注释，不能把两套文件说成相同 SHA256，不能混用装载包。

本次保留公开依赖原件，仅新增适配器、其合成测试和本文。原转换、抽取、字段解释与人工确认规则不因公开打包改变。
