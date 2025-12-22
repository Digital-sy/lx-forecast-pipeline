# 数据采集与分析系统

## 📂 项目结构

```
pythondata/
├── jobs/                         # 项目目录（每个需求一个文件夹）
│   └── purchase_analysis/       # 采购下单分析项目
│       ├── __init__.py
│       ├── main.py              # 主入口
│       ├── fetch_purchase.py    # 采集采购单
│       ├── fetch_operation.py   # 采集运营计划
│       └── generate_analysis.py # 生成分析表
│
├── common/                       # 公共模块（配置、数据库、日志）
├── utils/                        # 工具函数
├── lingxing/                     # 灵星API封装
└── scripts/                      # 部署脚本
```

## 🚀 快速开始

### 本地运行

```bash
# 1. 配置环境变量
copy env.example .env
# 编辑 .env 填入配置

# 2. 安装依赖
pip install -r requirements.txt

# 3. 运行项目（必须用 -m 参数）
python -m jobs.purchase_analysis.main
```

### 服务器部署

```bash
# 1. 初始化服务器
bash scripts/setup_server.sh

# 2. 配置环境变量
vim .env

# 3. 配置定时任务
crontab -e
# 添加：
0 2 * * * cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.purchase_analysis.main >> /opt/apps/pythondata/logs/cron.log 2>&1
```

## 📝 添加新项目

### 三步创建新项目

```bash
# 1. 创建项目文件夹
mkdir jobs/your_project

# 2. 创建必需文件
jobs/your_project/
├── __init__.py      # 项目说明
├── main.py          # 主入口（必需）
├── task1.py         # 具体任务
└── task2.py         # 具体任务

# 3. 运行项目
python -m jobs.your_project.main
```

### 代码模板

```python
# jobs/your_project/main.py
#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""项目主入口"""
import asyncio
from common import get_logger

logger = get_logger('your_project.main')

async def main():
    logger.info("开始执行...")
    
    # 导入并执行任务
    from .task1 import main as task1_main
    await task1_main()
    
    logger.info("执行完成")

if __name__ == "__main__":
    asyncio.run(main())
```

```python
# jobs/your_project/task1.py
#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""具体任务"""
import asyncio
from common import settings, get_logger
from common.database import db_cursor

logger = get_logger('your_project.task1')

async def main():
    logger.info("任务开始")
    # 您的代码逻辑
    logger.info("任务完成")

if __name__ == "__main__":
    asyncio.run(main())
```

## 🔧 核心功能

### 从API获取数据

```python
from lingxing import OpenApiBase

async def fetch_data():
    api = OpenApiBase()
    token = await api.get_access_token()
    resp = await api.request(token, '/api/path', params)
    return resp.get('data', [])
```

### 数据库操作

```python
from common.database import db_cursor

# 查询
def query_data():
    sql = "SELECT * FROM table WHERE field = %s"
    with db_cursor() as cursor:
        cursor.execute(sql, (value,))
        return cursor.fetchall()

# 插入
def insert_data(data):
    sql = "INSERT INTO table (field1, field2) VALUES (%(field1)s, %(field2)s)"
    with db_cursor() as cursor:
        cursor.executemany(sql, data)
```

## ⚠️ 注意事项

### ✅ 正确运行方式
```bash
python -m jobs.your_project.main  # 使用 -m 参数
```

### ❌ 错误运行方式
```bash
python jobs/your_project/main.py  # 会报错：ModuleNotFoundError
```

## 🔄 工作流程

```
本地开发 → Git提交 → 推送远程 → 服务器拉取 → 自动运行
```

```bash
# 本地
git add .
git commit -m "feat: 添加新项目"
git push origin main

# 服务器
ssh user@server
cd /opt/apps/pythondata
bash scripts/update_project.sh
```

## 📚 文档

- **[DEPLOY.md](DEPLOY.md)** - 服务器部署指南
- **[GIT_WORKFLOW.md](GIT_WORKFLOW.md)** - Git 工作流程
- **[FAQ.md](FAQ.md)** - 常见问题
- **[scripts/README.md](scripts/README.md)** - 脚本说明

---

**最后更新**: 2025-12-22
