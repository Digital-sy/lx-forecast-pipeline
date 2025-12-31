# 部署说明

## 推送到服务器

### 1. 提交代码到 Git

```bash
# 在本地项目目录
git add .
git commit -m "更新：添加销量预估表，修改定时任务为晚上10:30"
git push origin main  # 或你的分支名
```

### 2. 在服务器上拉取最新代码

```bash
# SSH 登录到服务器
ssh user@your-server

# 进入项目目录
cd /opt/apps/pythondata

# 拉取最新代码
git pull origin main  # 或你的分支名
```

### 3. 更新依赖（如果需要）

```bash
# 激活虚拟环境
source venv/bin/activate

# 更新依赖
pip install -r requirements.txt
```

## 配置定时任务（晚上10:30执行）

### 方法一：使用 crontab 配置（推荐）

1. **编辑 crontab**

```bash
crontab -e
```

2. **添加以下内容**（根据实际路径修改）

```bash
# 每天晚上 22:30 执行采购下单分析项目
30 22 * * * /opt/apps/pythondata/scripts/run_jobs.sh >> /opt/apps/pythondata/logs/cron_all.log 2>&1
```

3. **保存并退出**

   - 如果使用 `vi`：按 `Esc`，输入 `:wq`，按 `Enter`
   - 如果使用 `nano`：按 `Ctrl+X`，然后 `Y`，再按 `Enter`

4. **验证定时任务**

```bash
# 查看当前定时任务
crontab -l

# 应该能看到刚才添加的任务
```

### 方法二：直接执行 Python 命令

如果不想使用脚本，也可以直接在 crontab 中执行 Python 命令：

```bash
# 每天晚上 22:30 执行
30 22 * * * cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.purchase_analysis.main >> /opt/apps/pythondata/logs/cron_purchase_analysis.log 2>&1
```

## 验证部署

### 1. 检查脚本权限

```bash
chmod +x /opt/apps/pythondata/scripts/run_jobs.sh
```

### 2. 手动测试执行

```bash
# 手动执行一次，确保脚本正常
/opt/apps/pythondata/scripts/run_jobs.sh
```

### 3. 检查日志

```bash
# 查看执行日志
tail -f /opt/apps/pythondata/logs/cron_all.log

# 或查看 Python 日志
tail -f /opt/apps/pythondata/logs/purchase_analysis.main.log
```

## 定时任务说明

- **执行时间**：每天晚上 22:30（10:30 PM）
- **执行内容**：采购下单分析项目的所有任务
  - 采集FBA库存数据
  - 采集仓库库存明细
  - 采集销量统计数据
  - 采集采购单数据
  - 采集运营下单计划
  - 生成分析表
  - 生成库存预估表
  - 生成销量预估表

## 常见问题

### 1. 定时任务没有执行

- 检查 crontab 服务是否运行：`systemctl status cron`（Ubuntu/Debian）或 `systemctl status crond`（CentOS/RHEL）
- 检查日志文件是否有错误信息
- 检查脚本路径是否正确（使用绝对路径）

### 2. 权限问题

- 确保脚本有执行权限：`chmod +x scripts/run_jobs.sh`
- 确保日志目录存在且有写权限：`mkdir -p logs && chmod 755 logs`

### 3. Python 环境问题

- 确保虚拟环境路径正确
- 确保所有依赖已安装：`pip install -r requirements.txt`

## 修改执行时间

如果需要修改执行时间，编辑 crontab：

```bash
crontab -e
```

修改时间格式（分钟 小时）：
- `30 22` = 晚上 22:30
- `0 2` = 凌晨 2:00
- `0 0` = 凌晨 0:00

保存后，新的时间会在下一个周期生效。

