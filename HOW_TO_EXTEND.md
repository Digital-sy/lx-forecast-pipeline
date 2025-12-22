# 如何扩展项目

## 📦 核心理念

**一个需求 = 一个文件夹（项目） = 一个主函数**

## 🚀 三步添加新项目

### 1️⃣ 创建项目文件夹

```bash
mkdir jobs/your_project_name
```

### 2️⃣ 创建文件

```bash
jobs/your_project_name/
├── __init__.py          # 项目说明
├── main.py              # 主入口（必需）
├── task1.py             # 任务1
├── task2.py             # 任务2
└── task3.py             # 任务3...
```

### 3️⃣ 编写代码

#### `__init__.py` - 项目说明

```python
"""
项目名称

功能说明：
1. 功能1
2. 功能2
3. 功能3
"""
```

#### `main.py` - 主入口（必需）

```python
#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""项目主入口"""
import asyncio
from common import get_logger

logger = get_logger('your_project.main')

async def main():
    """主函数：执行所有任务"""
    logger.info("="*80)
    logger.info("项目开始执行")
    logger.info("="*80)
    
    try:
        # 导入并执行任务
        from .task1 import main as task1_main
        from .task2 import main as task2_main
        
        logger.info("[1/2] 执行任务1...")
        await task1_main()
        
        logger.info("[2/2] 执行任务2...")
        await task2_main()
        
        logger.info("="*80)
        logger.info("✅ 项目执行完成")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ 执行失败: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
```

#### `task1.py` - 具体任务

```python
#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""任务1：从API获取数据"""
import asyncio
from common import settings, get_logger
from common.database import db_cursor
from lingxing import OpenApiBase

logger = get_logger('your_project.task1')

async def fetch_data_from_api():
    """从API获取数据"""
    api = OpenApiBase()
    token = await api.get_access_token()
    resp = await api.request(token, '/api/path', {})
    return resp.get('data', [])

def save_to_database(data):
    """保存到数据库"""
    sql = "REPLACE INTO table_name (field1, field2) VALUES (%(field1)s, %(field2)s)"
    with db_cursor() as cursor:
        cursor.executemany(sql, data)
    logger.info(f"保存 {len(data)} 条数据")

async def main():
    """任务主函数"""
    logger.info("开始执行任务1")
    
    # 获取数据
    data = await fetch_data_from_api()
    
    # 保存数据
    save_to_database(data)
    
    logger.info("任务1完成")

if __name__ == "__main__":
    asyncio.run(main())
```

## ▶️ 运行项目

### 本地运行

```bash
# 运行整个项目
python -m jobs.your_project.main

# 或运行单个任务
python -m jobs.your_project.task1
```

### 服务器运行

```bash
# 1. 提交代码
git add jobs/your_project
git commit -m "feat: 添加新项目"
git push origin main

# 2. 服务器更新
ssh user@server
cd /opt/apps/pythondata
bash scripts/update_project.sh

# 3. 测试运行
python -m jobs.your_project.main

# 4. 配置定时任务
crontab -e
# 添加：
0 3 * * * cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.your_project.main >> /opt/apps/pythondata/logs/cron_your_project.log 2>&1
```

## 📋 完整示例项目

参考现有的采购分析项目：

```
jobs/purchase_analysis/
├── __init__.py              # 项目说明
├── main.py                  # 主入口
├── fetch_purchase.py        # 采集采购单
├── fetch_operation.py       # 采集运营计划
└── generate_analysis.py     # 生成分析表
```

运行方式：
```bash
python -m jobs.purchase_analysis.main
```

## 🔧 常用代码片段

### 从API获取数据

```python
from lingxing import OpenApiBase

async def fetch_data():
    api = OpenApiBase()
    token = await api.get_access_token()
    resp = await api.request(token, '/api/path', params)
    return resp.get('data', [])
```

### 从飞书获取数据

```python
from common.feishu import FeishuAPI

def fetch_feishu_data():
    api = FeishuAPI()
    records = api.fetch_all_records(
        app_token='your_app_token',
        table_id='your_table_id'
    )
    return records
```

### 数据库查询

```python
from common.database import db_cursor

def query_data():
    sql = "SELECT * FROM table WHERE field = %s"
    with db_cursor() as cursor:
        cursor.execute(sql, (value,))
        return cursor.fetchall()
```

### 数据库插入

```python
from common.database import db_cursor

def save_data(data_list):
    sql = "REPLACE INTO table (field1, field2) VALUES (%(field1)s, %(field2)s)"
    with db_cursor() as cursor:
        cursor.executemany(sql, data_list)
```

### 创建表

```python
from common.database import db_cursor

def create_table():
    sql = """
    CREATE TABLE IF NOT EXISTS table_name (
        id INT AUTO_INCREMENT PRIMARY KEY,
        field1 VARCHAR(100),
        field2 INT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_field1 (field1)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with db_cursor() as cursor:
        cursor.execute(sql)
```

## ⚠️ 注意事项

1. **必须使用 `-m` 参数运行**
   ```bash
   # 正确 ✅
   python -m jobs.your_project.main
   
   # 错误 ❌
   python jobs/your_project/main.py
   ```

2. **每个项目必须有 `__init__.py`**

3. **推荐每个项目有一个 `main.py` 作为入口**

4. **日志命名建议**：`项目名.任务名`
   ```python
   logger = get_logger('your_project.task1')
   ```

## 📚 参考资料

- 查看现有项目：`jobs/purchase_analysis/`
- 部署指南：`DEPLOY.md`
- Git 工作流程：`GIT_WORKFLOW.md`
- 常见问题：`FAQ.md`

---

**就是这么简单！** 创建文件夹 → 写代码 → 运行测试 → 推送服务器 ✅

