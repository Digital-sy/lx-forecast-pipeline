# 服务器部署指南

## 📦 部署步骤

### 1. 服务器环境准备

```bash
# 更新系统
sudo apt update && sudo apt upgrade -y  # Ubuntu/Debian
# 或
sudo yum update -y  # CentOS/AliyunLinux

# 安装 Python 3.8+
sudo apt install python3 python3-pip python3-venv -y  # Ubuntu/Debian
# 或
sudo yum install python3 python3-pip -y  # CentOS

# 安装 Git
sudo apt install git -y  # Ubuntu/Debian
# 或
sudo yum install git -y  # CentOS

# 安装 MySQL 客户端库（如果需要）
sudo apt install libmysqlclient-dev -y  # Ubuntu/Debian
# 或
sudo yum install mysql-devel -y  # CentOS
```

### 2. 克隆项目

```bash
# 选择一个合适的目录
cd /opt  # 或者 /home/your_user
sudo mkdir -p apps
cd apps

# 克隆项目
sudo git clone <your_git_repository_url> pythondata
sudo chown -R $USER:$USER pythondata
cd pythondata
```

### 3. 创建虚拟环境并安装依赖

```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate

# 升级 pip
pip install --upgrade pip

# 安装依赖
pip install -r requirements.txt
```

### 4. 配置环境变量

```bash
# 复制配置模板
cp env.example .env

# 编辑配置文件
vim .env  # 或使用 nano .env

# 填入真实的配置信息：
# - 数据库连接信息
# - 灵星API密钥
# - 飞书API密钥
# - 其他必要配置
```

### 5. 测试运行

```bash
# 确保虚拟环境已激活
source venv/bin/activate

# 测试各个任务
python -m jobs.purchase_order
python -m jobs.operation_order
python -m jobs.analysis_table

# 检查日志
tail -f logs/$(date +%Y-%m-%d)/*.log
```

## ⏰ 配置定时任务

### 方式一：使用 Crontab（推荐）

```bash
# 编辑 crontab
crontab -e

# 添加以下内容（根据实际需求调整时间）：
# 每天凌晨 2:00 执行采购单采集
0 2 * * * cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.purchase_order >> /opt/apps/pythondata/logs/cron_purchase.log 2>&1

# 每天凌晨 3:00 执行运营下单采集
0 3 * * * cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.operation_order >> /opt/apps/pythondata/logs/cron_operation.log 2>&1

# 每天凌晨 4:00 执行分析表生成
0 4 * * * cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.analysis_table >> /opt/apps/pythondata/logs/cron_analysis.log 2>&1
```

### 方式二：创建部署脚本

创建 `scripts/run_jobs.sh`：

```bash
#!/bin/bash

# 设置项目路径
PROJECT_DIR="/opt/apps/pythondata"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON="$VENV_DIR/bin/python"

# 切换到项目目录
cd "$PROJECT_DIR"

# 激活虚拟环境
source "$VENV_DIR/bin/activate"

# 记录开始时间
echo "==================================="
echo "任务开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="

# 执行采购单采集
echo "开始执行: 采购单数据采集..."
$PYTHON -m jobs.purchase_order
if [ $? -eq 0 ]; then
    echo "✓ 采购单数据采集完成"
else
    echo "✗ 采购单数据采集失败"
fi

# 执行运营下单采集
echo "开始执行: 运营下单数据采集..."
$PYTHON -m jobs.operation_order
if [ $? -eq 0 ]; then
    echo "✓ 运营下单数据采集完成"
else
    echo "✗ 运营下单数据采集失败"
fi

# 执行分析表生成
echo "开始执行: 分析表生成..."
$PYTHON -m jobs.analysis_table
if [ $? -eq 0 ]; then
    echo "✓ 分析表生成完成"
else
    echo "✗ 分析表生成失败"
fi

# 记录结束时间
echo "==================================="
echo "任务结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================="
```

然后设置定时任务：

```bash
# 给脚本添加执行权限
chmod +x scripts/run_jobs.sh

# 添加到 crontab
crontab -e

# 每天凌晨 2:00 执行所有任务
0 2 * * * /opt/apps/pythondata/scripts/run_jobs.sh >> /opt/apps/pythondata/logs/cron_all.log 2>&1
```

## 🔄 更新代码流程

```bash
# 切换到项目目录
cd /opt/apps/pythondata

# 拉取最新代码
git pull origin main  # 或 master，根据您的分支名

# 激活虚拟环境
source venv/bin/activate

# 更新依赖（如果 requirements.txt 有变化）
pip install -r requirements.txt --upgrade

# 重启定时任务（如果需要）
# 如果使用 systemd 服务，执行：
# sudo systemctl restart pythondata.service
```

## 📊 监控和维护

### 查看日志

```bash
# 查看实时日志
tail -f logs/$(date +%Y-%m-%d)/*.log

# 查看 crontab 执行日志
tail -f logs/cron_*.log

# 查看某个任务的历史日志
ls -lh logs/*/purchase_order.log
```

### 检查任务执行情况

```bash
# 查看 crontab 任务列表
crontab -l

# 查看系统 cron 日志
sudo grep CRON /var/log/syslog  # Ubuntu/Debian
# 或
sudo tail -f /var/log/cron  # CentOS
```

### 磁盘空间管理

```bash
# 定期清理旧日志（保留最近 30 天）
find /opt/apps/pythondata/logs -type f -name "*.log" -mtime +30 -delete

# 或者添加到 crontab，每周清理一次
0 0 * * 0 find /opt/apps/pythondata/logs -type f -name "*.log" -mtime +30 -delete
```

## 🔐 安全建议

1. **环境变量保护**
   ```bash
   # 确保 .env 文件权限正确
   chmod 600 .env
   ```

2. **使用专用用户**
   ```bash
   # 创建专用用户运行应用
   sudo useradd -m -s /bin/bash pythondata
   sudo chown -R pythondata:pythondata /opt/apps/pythondata
   
   # 使用该用户的 crontab
   sudo -u pythondata crontab -e
   ```

3. **Git 配置**
   ```bash
   # 避免在服务器上提交代码
   git config --local core.fileMode false
   
   # 如果需要拉取私有仓库，配置 SSH 密钥或访问令牌
   ```

## 🐛 故障排查

### 常见问题

1. **模块导入错误**
   - 确保在项目根目录执行
   - 确保虚拟环境已激活
   - 检查 PYTHONPATH

2. **数据库连接失败**
   - 检查 .env 配置
   - 确认数据库服务运行中
   - 检查防火墙规则
   - 测试网络连通性：`mysql -h <host> -u <user> -p`

3. **API 调用失败**
   - 检查网络连接
   - 确认代理配置（如需要）
   - 检查 API 密钥是否正确

4. **定时任务不执行**
   - 检查 crontab 语法
   - 检查文件路径是否正确
   - 查看系统日志：`sudo tail -f /var/log/syslog`
   - 确认 cron 服务运行：`sudo systemctl status cron`

### 调试方法

```bash
# 手动执行脚本，查看详细输出
cd /opt/apps/pythondata
source venv/bin/activate
python -m jobs.purchase_order

# 检查环境变量是否加载
python -c "from common.config import settings; print(settings.DB_HOST)"

# 测试数据库连接
python -c "from common.database import DatabaseManager; db = DatabaseManager(); db.test_connection()"
```

## 📞 联系支持

如遇到问题，请查看日志文件或联系开发团队。

