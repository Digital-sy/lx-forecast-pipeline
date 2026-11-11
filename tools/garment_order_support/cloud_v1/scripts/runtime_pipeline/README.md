# 可部署下单表支撑源读取流水线

独立副本版本 `1.2.1-bot-snapshot-fail-stop`。旧源码及原件不修改。本子任务没有网络、数据库或部署操作。

运行包：本目录4个根Python文件、`tools/`、`runtime_manifest.json`。保留工具与解析器相对布局；`tools/dependency_manifest.json`固定新版解析器、helper、wrapper和既有xlrd依赖。新解析器为 `1.1.0-body-yyyymmdd`。

配置采用 `garment_source_pipeline/2`，见 `config.example.json`。所有config path相对config文件所在目录；解析依赖相对其manifest。`discovery.identity`必须明确为`bot`，发现与下载命令均传`--as bot`并检查响应身份；`cli_executable`与`cli_script`可指向Python及主任务提供的只读cloud shim。本包不加载凭据，不配置认证，不实现新API。

```text
python -B source_pipeline_v2.py --config CONFIG.json --output-dir NEW_CHECK --stage CHECK_ONLY
python -B source_pipeline_v2.py --config CONFIG.json --output-dir NEW_DISCOVERY --stage DISCOVER --execute-read
python -B source_pipeline_v2.py --config CONFIG.json --output-dir NEW_PREPARE --stage PREPARE --execute-read --discovery-run NEW_DISCOVERY
```

只读快照直接使用publisher生成的原`receipt.json`，支持真实`READ_ONLY_SNAPSHOT_VERIFIED`和具备相同验证字段的已提交回执。核对固定schema/UUID、9表、metadata与structure、行数、总数及canonical hashes。只读状态不改写成COMMITTED，不制造final_readback。PREPARE把该原文件直接传给builder的`--production-before`，package记录它的真实SHA，供publisher随后绑定同一原文件。

日期一致性门禁及RAW_ONLY范围保持：未解析正文/冲突不得误用文件名日期；新增包不写面料解释、人工确认及人工引用三表。真实parser与原件均绑定hash。builder不访问数据库，流水线不提供发布命令。

顶层`reparse_queue`固定为null或`{path,sha256}`。队列契约 `garment_reparse_queue/1`，每项明确source_id、file类型、PENDING、内容SHA、旧/新解析版本及原因。存在已知待修订而没有普通增量时，返回 `REPARSE_REQUIRED_NOT_PREPARED`，不伪装成无事可做；普通增量可继续，队列保持不变且回执保留pending数量和条目。

ZSY967已知队列在`private/reparse_queue_zsy967.json`。它只表示旧6行需要独立追加解析版本，不会自动发布迁移。原因：旧builder的获取run标识绑定原获取回执，直接替换其解析回执会造成旧run同ID异内容；必须按独立迁移方案保留原获取、原解释及人工决策。本版不伪造新获取时间，不自动清队列。

验证：`python -B -m unittest discover -s tests -v`。44项合成测试通过（原调用链24项＋新增20项），详见`verification/runtime_tests_v2.json/.log`。这证明本地代码与合同检查，不等于真实bot/数据库集成已执行。真实部署和外层日调度由主任务验收。

打包代码时排除`private/`、`temp/`及业务回执；ZSY967队列由受控配置单独管理，不能因此遗失待办。不要将业务响应或凭据上传Git。

获取阶段任一API错误、认证错误或本地文件处理错误即停止后续源文件，后续记为NOT_RUN/STOPPED_AFTER_SOURCE_FAILURE；原错误类型与代码保留，不伪称全部为认证失败。没有自动重试，不因本地检查成功宣称真实云端请求已经通过。
