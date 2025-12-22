# Scripts 目录说明

这个目录包含了用于服务器部署、运行和维护的脚本文件。

## 📁 文件列表

### 部署脚本

#### `setup_server.sh`
**用途**：在新服务器上初始化项目环境

**功能**：
- 检查 Python 和 Git 环境
- 创建虚拟环境
- 安装项目依赖
- 配置环境变量文件
- 设置文件权限

**使用方法**：
```bash
cd /opt/apps/pythondata
bash scripts/setup_server.sh
```

---

#### `update_project.sh`
**用途**：从 Git 仓库更新项目代码

**功能**：
- 拉取最新代码
- 更新 Python 依赖
- 检查配置文件
- 安全性检查（检测未提交的更改）

**使用方法**：
```bash
cd /opt/apps/pythondata
bash scripts/update_project.sh
```

---

### 运行脚本

#### `run_jobs.sh`
**用途**：执行所有数据采集任务

**功能**：
- 按顺序执行采购单采集
- 执行运营下单采集
- 执行分析表生成
- 记录任务执行日志

**使用方法**：
```bash
# 手动执行
cd /opt/apps/pythondata
bash scripts/run_jobs.sh

# 或通过 crontab 定时执行
0 2 * * * /opt/apps/pythondata/scripts/run_jobs.sh >> /opt/apps/pythondata/logs/cron_all.log 2>&1
```

---

### 监控脚本

#### `check_status.sh`
**用途**：检查项目运行状态和配置

**功能**：
- 检查 Python 环境和依赖
- 检查配置文件
- 检查日志目录大小
- 检查 Git 状态
- 检查定时任务配置
- 测试数据库连接

**使用方法**：
```bash
cd /opt/apps/pythondata
bash scripts/check_status.sh
```

---

### 配置文件

#### `crontab.example`
**用途**：Crontab 定时任务配置示例

**内容**：
- 多种定时执行方案
- 常用时间配置示例
- 日志清理任务配置
- 详细的注释说明

**使用方法**：
```bash
# 查看示例
cat scripts/crontab.example

# 编辑 crontab
crontab -e

# 将需要的配置复制到 crontab 中，并修改实际路径
```

---

## 🚀 快速开始

### 1. 首次部署

```bash
# 克隆项目
git clone <repository_url> /opt/apps/pythondata
cd /opt/apps/pythondata

# 运行初始化脚本
bash scripts/setup_server.sh

# 编辑配置文件
vim .env

# 测试运行
source venv/bin/activate
python -m jobs.purchase_order
```

### 2. 配置定时任务

```bash
# 给脚本添加执行权限
chmod +x scripts/*.sh

# 编辑 crontab
crontab -e

# 添加定时任务（参考 scripts/crontab.example）
0 2 * * * /opt/apps/pythondata/scripts/run_jobs.sh >> /opt/apps/pythondata/logs/cron_all.log 2>&1
```

### 3. 日常更新

```bash
cd /opt/apps/pythondata
bash scripts/update_project.sh
```

### 4. 检查状态

```bash
cd /opt/apps/pythondata
bash scripts/check_status.sh
```

---

## 📝 脚本使用最佳实践

### 1. 权限管理

```bash
# 确保脚本有执行权限
chmod +x scripts/*.sh

# 确保环境变量文件受保护
chmod 600 .env
```

### 2. 日志管理

所有脚本都应该将输出重定向到日志文件：

```bash
# 方式一：追加到日志
bash scripts/run_jobs.sh >> logs/cron_all.log 2>&1

# 方式二：单独的错误日志
bash scripts/run_jobs.sh >> logs/cron_all.log 2>> logs/cron_error.log
```

### 3. 使用专用用户

```bash
# 创建专用用户
sudo useradd -m -s /bin/bash pythondata
sudo chown -R pythondata:pythondata /opt/apps/pythondata

# 使用该用户运行
sudo -u pythondata bash scripts/run_jobs.sh
```

### 4. 错误处理

脚本中已包含基本的错误处理：
- `set -e`: 遇到错误立即退出
- 返回码检查: `if [ $? -eq 0 ]`
- 路径检查: `cd "$PROJECT_DIR" || exit 1`

### 5. 监控和告警

```bash
# 方案一：通过 crontab 邮件通知（需配置 sendmail）
MAILTO=your-email@example.com
0 2 * * * /opt/apps/pythondata/scripts/run_jobs.sh

# 方案二：在脚本中添加告警逻辑
# 可以集成钉钉、飞书、企业微信等通知
```

---

## 🔧 自定义和扩展

### 添加新的数据采集任务

1. 在 `jobs/` 目录创建新的任务文件
2. 修改 `run_jobs.sh` 添加新任务的执行
3. 在 `crontab.example` 添加新任务的示例配置

### 添加监控检查项

修改 `check_status.sh`，添加新的检查逻辑：

```bash
# 检查某个特定表的数据
echo ""
echo "【数据检查】"
python -c "
from common.database import DatabaseManager
db = DatabaseManager()
count = db.execute_query('SELECT COUNT(*) FROM your_table')
print(f'表记录数: {count}')
"
```

### 添加数据清理脚本

创建新的清理脚本 `cleanup_old_data.sh`：

```bash
#!/bin/bash
# 清理 90 天前的历史数据
cd /opt/apps/pythondata
source venv/bin/activate
python -c "
from common.database import DatabaseManager
db = DatabaseManager()
db.execute_update('DELETE FROM purchase_orders WHERE created_at < DATE_SUB(NOW(), INTERVAL 90 DAY)')
print('清理完成')
"
```

然后在 crontab 中添加：
```bash
0 2 1 * * /opt/apps/pythondata/scripts/cleanup_old_data.sh >> /opt/apps/pythondata/logs/cleanup.log 2>&1
```

---

## 🐛 故障排查

### 脚本执行失败

1. **检查权限**
   ```bash
   ls -la scripts/
   # 确保文件有执行权限 (-rwxr-xr-x)
   ```

2. **检查路径**
   ```bash
   # 确保脚本中的 PROJECT_DIR 正确
   head -n 20 scripts/run_jobs.sh | grep PROJECT_DIR
   ```

3. **检查环境**
   ```bash
   # 手动执行脚本查看详细错误
   bash -x scripts/run_jobs.sh
   ```

### Crontab 不执行

1. **检查 cron 服务**
   ```bash
   sudo systemctl status cron  # Ubuntu/Debian
   # 或
   sudo systemctl status crond  # CentOS
   ```

2. **查看 cron 日志**
   ```bash
   sudo tail -f /var/log/syslog | grep CRON  # Ubuntu/Debian
   # 或
   sudo tail -f /var/log/cron  # CentOS
   ```

3. **检查环境变量**
   ```bash
   # crontab 环境变量可能不完整，在脚本开头添加：
   export PATH=/usr/local/bin:/usr/bin:/bin
   source ~/.bashrc
   ```

---

## 📚 参考资料

- [Crontab Guru](https://crontab.guru/) - Crontab 表达式在线工具
- [Bash 脚本编程指南](https://www.gnu.org/software/bash/manual/)
- 项目部署文档：`../DEPLOY.md`

---

## 👥 维护

如需修改或添加新脚本，请遵循以下规范：

1. 添加详细的注释
2. 使用 `set -e` 进行错误处理
3. 输出清晰的日志信息
4. 更新此 README 文档
5. 在 `crontab.example` 中添加使用示例

