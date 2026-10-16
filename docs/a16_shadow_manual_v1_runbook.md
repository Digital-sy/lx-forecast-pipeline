# A16 V3｜JQ-US H2/H3 单次受控影子执行手册

授权口径：2026-10-16，用户批准在非生产路径对 JQ-US / ESTABLISHED / H2,H3 做一次人工触发的观察性影子测试。没有授权替换V4、改采购、写正式数据库、创建新调度或更新BI。

## 执行实现

- 冻结候选 a16_shop_cutoff_v3_20261016 的源码和manifest不修改。
- 执行文件：scripts/run_a16_shadow_once.py；仅执行月份必须为服务器当前 Asia/Shanghai 月份。
- 2026-10签发：H2目标为2026-12，H3目标为2027-01；双方只看严格早于2026-10-01的完整月销量。
- V4使用冻结源码中的纯函数在相同输入重算；其结果是 v4_replay，不是已有生产已签发不可变V4快照。
- 当月运行时未来目标月实际值根本不在输入SQL中，影子不计算WAPE。
- 数据源仅限 销量统计_msku月度（店铺JQ-US、2024-01至签发月前）、ods_db.ods_lx_product_management（当前SPU品类）、SPU季节表（当前季节）。暂不读取日明细大表。
- 数据库在只读一致性事务中查询，查询前运行原生 EXPLAIN FORMAT=JSON，月表/商品表/季节表分别限制预估扫描不超过1000000/200000/20000行，返回行数不超过400000/100000/10000，单查询60秒。执行计划超预算则停止，不提高额度自动重试。
- 校验模型源码哈希，SKU↔SPU无冲突，最近3个完整月份有记录，实际未知不补零，合格对象A16输出覆盖100%，训练目标月早于截点。
- 仅在本地新建 reports_analysis/a16_shadow_manual/manual_* 模式700目录；输出模型明细CSV（敏感运营数据，仅服务器保留）和状态JSON。不会更新任何现有生产文件，也没有后台调度和网络发布。
- 输入保留缺失状态；source_sync_at=UNKNOWN_NOT_VERIFIED，因此完成只表示计算观察成功，不证明领星/BI来源新鲜度已认证。

## 在脏工作树中的安全获取

服务器当前工作分支可能不是main，且保留未提交的生产修改。因此严禁 git pull、git checkout、git reset --hard、git clean -fd。

从本分支仅复制白名单四个新文件，遇到本地同名不同内容即停止：
- docs/a16_candidate_v3_manifest.json
- scripts/a16_candidate_v3.py
- scripts/run_a16_shadow_once.py
- tests/test_a16_shadow_once.py

执行前确认业务时间为2026年10月；否则重新申请当月签发范围。获取代码后先进行 Python 语法检查、单元测试，PASS后再人工单次读取与预测。

## 运行输出验收

成功：shadow_run_meta.json 中状态 COMPLETE_OBSERVATION_ONLY，shop=JQ-US，只有H2/H3，target分别为2026-12/2027-01，两个时距均 a16_output_count=eligible_count，输出数量非负，已对齐输入哈希、训练截止与当前源码，CSV记录里不含实际目标月销量。

阻断：FAILED_CLOSED，记录错误类别，不允许将已产生的局部文件当成成功预测、也不得修改V4进行补救。未经审批不改变查询预算、候选哈希或截止规则。

既有V4仍唯一供给正式采购。此执行不构成生产灰度，真实准确率需待2026-12和2027-01自然月结束后依据可信真值另做前瞻评估。重点审视BX389/BX517/BX422退化风险，但不得基于未来实际自动切换模型。

最终报告仅需提交：运行目录、状态JSON中的status/input_hash/models/planned_scan_rows/output_rows和单元测试输出；含SPU预测的CSV不得推送GitHub。

## 未解决限制

- 同源V4回放不等于线上真实发布V4；若需生产签发对照，必须另行认证当日已签发快照的业务月份、时间、覆盖与版本。
- 当前品类/季节不是历史时点属性；日期真实值无提前泄漏，源表后补/修订无法完全复原。
- 数据源同步时效尚未另行核证；COMPLETE_OBSERVATION_ONLY不等于模型获准进入生产。
- GitHub代码提交与静态审阅不是服务器实测。
