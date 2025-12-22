# 灵星数据采集与分析系统

> 💡 **服务器部署？** 请查看 [服务器部署指南](DEPLOY.md) 和 [脚本使用说明](scripts/README.md)

## 📋 项目简介

这是一个基于灵星(Asinking)电商管理平台API的数据采集与分析系统，主要功能包括：

- 📦 **采购单数据采集**：从灵星API获取采购订单数据
- 📊 **运营下单数据采集**：从飞书多维表格获取运营预计下单数据
- 📈 **下单分析报表**：对比实际采购与预计下单，生成分析报表

## 🚀 快速导航

- 📖 [本地开发环境搭建](#-快速开始)
- 🖥️ [服务器部署指南](DEPLOY.md)
- 📜 [部署脚本说明](scripts/README.md)
- ⚙️ [配置说明](#-配置说明)

## 🏗️ 项目结构

```
pythondata/
├── lingxing/                 # 灵星API SDK封装
│   ├── __init__.py
│   ├── aes.py               # AES加密工具
│   ├── http_util.py         # HTTP请求封装
│   ├── openapi.py           # OpenAPI基础操作
│   ├── resp_schema.py       # 响应数据模型
│   ├── seller_mapping.py    # 店铺映射
│   └── sign.py              # API签名算法
│
├── common/                   # 公共工具模块
│   ├── __init__.py
│   ├── config.py            # 统一配置管理
│   ├── database.py          # 数据库连接管理
│   ├── logger.py            # 日志系统
│   └── feishu.py            # 飞书API封装
│
├── jobs/                     # 数据采集任务
│   ├── __init__.py
│   ├── purchase_order.py    # 采购单数据采集
│   ├── operation_order.py   # 运营下单数据采集
│   └── analysis_table.py    # 下单分析表生成
│
├── utils/                    # 工具函数
│   ├── __init__.py
│   ├── date_utils.py        # 日期处理工具
│   └── data_transform.py    # 数据转换工具
│
├── scripts/                  # 部署和运行脚本
│   ├── setup_server.sh      # 服务器初始化脚本
│   ├── update_project.sh    # 项目更新脚本
│   ├── run_jobs.sh          # 任务执行脚本
│   ├── check_status.sh      # 状态检查脚本
│   ├── crontab.example      # 定时任务示例
│   └── README.md            # 脚本使用说明
│
├── logs/                     # 日志文件目录
├── .env                      # 环境变量配置（不提交到Git）
├── env.example               # 环境变量示例
├── .gitignore               # Git忽略配置
├── requirements.txt          # Python依赖
├── README.md                 # 项目文档
└── DEPLOY.md                 # 服务器部署指南
```

## 🚀 快速开始

### 1. 环境要求

- Python >= 3.8
- MySQL 5.7+
- 访问灵星API的网络环境（可能需要代理）

### 2. 安装依赖

```bash
# 克隆项目
git clone <repository_url>
cd pythondata

# 创建虚拟环境（推荐）
python -m venv venv

# 激活虚拟环境
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 3. 配置环境变量

```bash
# 复制环境变量示例文件
copy env.example .env  # Windows
# 或
cp env.example .env    # Linux/Mac

# 编辑 .env 文件，填入真实配置
notepad .env  # Windows
# 或
vim .env      # Linux/Mac
```

### 4. 运行任务

```bash
# 采集采购单数据
python -m jobs.purchase_order

# 采集运营下单数据
python -m jobs.operation_order

# 生成下单分析表
python -m jobs.analysis_table
```

## 📊 数据库表结构

### 采购单表
- SKU、FNSKU、店铺名、仓库、供应商
- 实际数量、创建时间、状态等

### 运营下单表
- SKU、店铺、下单数量
- 下单人、所属部门、下单时间等

### 下单分析表
- SKU、店铺、面料、月份
- 实际已下单数量、预计下单数量、下单差值
- 下单人、所属部门、更新时间

## 🔧 配置说明

### 灵星API配置
在 `.env` 中配置：
- `LINGXING_HOST`: API地址
- `LINGXING_APP_ID`: 应用ID
- `LINGXING_APP_SECRET`: 应用密钥
- `LINGXING_PROXY_URL`: 代理地址（如需要）

### 数据库配置
在 `.env` 中配置：
- `DB_HOST`: 数据库主机地址
- `DB_PORT`: 端口（默认3306）
- `DB_USER`: 用户名
- `DB_PASSWORD`: 密码
- `DB_DATABASE`: 数据库名

### 飞书配置
在 `.env` 中配置：
- `FEISHU_APP_ID`: 飞书应用ID
- `FEISHU_APP_SECRET`: 飞书应用密钥
- `FEISHU_APP_TOKEN`: 多维表格ID
- `FEISHU_TABLE_ID`: 表格ID
- `FEISHU_VIEW_ID`: 视图ID

## 📝 开发指南

### 添加新的数据采集任务

1. 在 `jobs/` 目录下创建新的Python文件
2. 使用 `common.config` 获取配置
3. 使用 `common.logger` 记录日志
4. 使用 `common.database` 操作数据库

### 代码规范

- 使用Python 3.8+语法特性
- 遵循PEP 8代码风格
- 添加类型注解
- 编写文档字符串
- 使用异步编程（asyncio）

## 🐛 常见问题

常见问题的快速解答，详细内容请查看 [FAQ.md](FAQ.md)

### 1. 代理连接失败
检查 `LINGXING_PROXY_URL` 是否正确，格式：`http://username:password@host:port`

### 2. 数据库连接失败
- 检查数据库配置是否正确
- 确认网络是否可达
- 检查数据库用户权限

### 3. 模块导入错误
确保在项目根目录运行脚本，或使用 `python -m` 方式运行

更多问题请参考 [常见问题解答 FAQ.md](FAQ.md)

## 📚 完整文档索引

### 📖 核心文档
- **[README.md](README.md)** - 项目总览和快速开始（本文档）
- **[DEPLOY.md](DEPLOY.md)** - 服务器部署完整指南
- **[FAQ.md](FAQ.md)** - 常见问题解答
- **[GIT_WORKFLOW.md](GIT_WORKFLOW.md)** - Git 使用和工作流程

### 🔧 配置文件
- **[env.example](env.example)** - 环境变量配置模板
- **[requirements.txt](requirements.txt)** - Python 依赖包列表
- **[.gitignore](.gitignore)** - Git 忽略规则

### 📜 脚本文档
- **[scripts/README.md](scripts/README.md)** - 部署脚本使用说明
- **[scripts/setup_server.sh](scripts/setup_server.sh)** - 服务器初始化脚本
- **[scripts/update_project.sh](scripts/update_project.sh)** - 项目更新脚本
- **[scripts/run_jobs.sh](scripts/run_jobs.sh)** - 定时任务执行脚本
- **[scripts/check_status.sh](scripts/check_status.sh)** - 状态检查脚本
- **[scripts/crontab.example](scripts/crontab.example)** - Crontab 配置示例

### 🗂️ 模块文档
- **[lingxing/README.md](lingxing/README.md)** - 灵星 API SDK 说明
- **[SHOP_MAPPING_NOTE.md](SHOP_MAPPING_NOTE.md)** - 店铺映射说明

## 📊 项目工作流程

```
本地开发 → Git 提交 → 推送到远程仓库 → 服务器拉取更新 → 定时任务执行
   ↓           ↓              ↓                    ↓                ↓
 编写代码    git commit    git push          git pull         crontab
           测试通过      代码审查          运行脚本         自动运行
```

## 🚀 快速命令参考

### 本地开发

```bash
# 激活环境
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# 运行任务
python -m jobs.purchase_order
python -m jobs.operation_order
python -m jobs.analysis_table

# 测试配置
python -c "from common.config import settings; print(settings.db_config)"
```

### 服务器部署

```bash
# 初次部署
bash scripts/setup_server.sh

# 更新代码
bash scripts/update_project.sh

# 检查状态
bash scripts/check_status.sh

# 手动运行任务
bash scripts/run_jobs.sh

# 查看日志
tail -f logs/$(date +%Y-%m-%d)/*.log
```

### Git 操作

```bash
# 提交更改
git add .
git commit -m "feat: 描述您的更改"
git push origin main

# 服务器更新
ssh user@server
cd /opt/apps/pythondata
git pull origin main
```

## 📄 许可证

内部项目，请勿外传。

## 👥 维护者

- 开发团队

## 📞 联系方式

如有问题，请：
1. 查看 [常见问题解答 FAQ.md](FAQ.md)
2. 运行状态检查：`bash scripts/check_status.sh`
3. 查看日志文件：`logs/` 目录
4. 联系开发团队

---

**最后更新**: 2025-12-22

