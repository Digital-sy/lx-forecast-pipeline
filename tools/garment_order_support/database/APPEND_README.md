# 九表追加装载核心

本增量仅提供追加库及合成测试。没有生产连接配置、远程执行入口、业务装载包或调度。调用者必须另行核验目标实例、授权范围、结构及触发器，并可靠保存完整旧快照。

## 合同

- 仅允许既定九张支撑表；单次旧、新合并范围最多 25,000 行。
- 仅 INSERT；同主键相同内容跳过，内容冲突停止。没有 UPSERT、UPDATE、DELETE、DDL 或写入重试。
- 复用首次装载锁命名空间；事务内以 SERIALIZABLE 和有界 FOR UPDATE 核验完整旧快照。
- 旧人工字段及创建、更新时间逐字段保留；新时间由数据库产生并回读检查。
- 包内 JSON 使用 Python 原生值；数据库快照 JSON 为编码字符串，不能混淆字符串标量、JSON null 和 SQL NULL。
- 提交确认丢失返回 COMMIT_UNKNOWN，并通过新连接回读填写 commit_resolution；调用者不得盲目重复写入。
- 合成测试中的连接、事务、故障均为测试替身，不代表真实数据库验收。生产故障注入被禁止。

公开基线 mysql_load_core、schema_ops、build_load_package 和 SQL 原样保留，仍绑定公开 DDL 哈希。不要将私有旧 DDL 哈希的装载包直接用于此公开副本。现有 release_manifest.json 是此前公开版本的历史清单，本增量不重写该历史回执；新增文件以本轮增量清单为准。

## 复现合成测试

在 tools/garment_order_support/database 目录下，使用已具备的 Python 环境：

```powershell
$env:GARMENT_SOURCE_DDL = Join-Path (Get-Location) 'sql/lingxing_garment_order_schema_v1_1.sql'
python -B -m unittest discover -s tests -p 'test_append_load_core.py' -v
```

此命令不读取业务数据，不建立数据库或网络连接。它验证追加范围、同键冲突、关系完整性、JSON 标量、人工字段与审计时间保护、回滚及提交不确定状态的模拟处理。生产执行需要受控私有调用方，不在本目录提供。
