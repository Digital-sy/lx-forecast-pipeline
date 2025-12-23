# 🚀 部署上线检查清单

## 📝 上线流程（完整版）

### 阶段一：本地准备 ✅

#### 1. 最后测试（必须）

```bash
# 切换到项目目录
cd e:\pythondata

# 测试运行主项目
python -m jobs.purchase_analysis.main

# 测试店铺映射
python -m jobs.purchase_analysis.shop_mapping
```

**预期结果**：
- ✅ 能成功获取20+个店铺映射
- ✅ RR开头的店铺统一为RR-EU
- ✅ 采购单、运营单、分析表都能正常生成

---

### 阶段二：提交到Git 📤

#### 2. 检查Git状态

```bash
# 查看当前状态
git status

# 查看修改了哪些文件
git diff
```

#### 3. 添加文件到Git

```bash
# 添加所有修改
git add .

# 或者选择性添加（推荐，更安全）
git add jobs/purchase_analysis/
git add common/
git add utils/
git add lingxing/
git add scripts/
git add requirements.txt
git add env.example
git add README.md
git add INSTALL.md
git add HOW_TO_EXTEND.md
git add DEPLOY.md
git add GIT_WORKFLOW.md
git add FAQ.md
git add .gitignore

# 查看将要提交的文件
git status
```

#### 4. 提交代码

```bash
# 提交（使用有意义的提交信息）
git commit -m "feat: 完成采购下单分析项目优化

主要更新：
- 重构项目结构，一个需求一个文件夹
- 新增店铺映射自动获取功能（从API获取20+个店铺）
- 实现RR开头店铺统一为RR-EU规则
- 优化依赖包配置，避免版本冲突
- 完善部署文档和使用说明
"

# 查看提交历史
git log --oneline -1
```

#### 5. 推送到远程仓库

```bash
# 推送到main分支（或master，根据您的仓库）
git push origin main

# 或如果是master分支
# git push origin master

# 查看远程状态
git remote -v
```

**✅ 完成本地部分！代码已上传到Git仓库**

---

### 阶段三：服务器部署 🖥️

#### 6. 登录服务器

```bash
# SSH登录到阿里云服务器
ssh your_username@your_server_ip

# 例如：
# ssh root@123.456.789.0
# 或
# ssh ubuntu@your-domain.com
```

#### 7. 首次部署（如果是新服务器）

```bash
# 克隆项目
cd /opt/apps
git clone <your_git_repository_url> pythondata

# 进入项目目录
cd pythondata

# 运行初始化脚本
bash scripts/setup_server.sh

# 配置环境变量
vim .env
# 按 i 进入编辑模式
# 填入真实的配置信息：
# - LINGXING_APP_ID
# - LINGXING_APP_SECRET
# - DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE
# - FEISHU_APP_ID, FEISHU_APP_SECRET
# 按 ESC，输入 :wq 保存退出
```

#### 8. 更新部署（如果已有项目）

```bash
# 进入项目目录
cd /opt/apps/pythondata

# 拉取最新代码
git pull origin main

# 激活虚拟环境
source venv/bin/activate

# 更新依赖（如果requirements.txt有变化）
pip install -r requirements.txt --upgrade

# 测试运行
python -m jobs.purchase_analysis.shop_mapping
```

#### 9. 测试运行

```bash
# 测试店铺映射
python -m jobs.purchase_analysis.shop_mapping

# 测试完整项目
python -m jobs.purchase_analysis.main

# 查看日志
tail -f logs/$(date +%Y-%m-%d)/purchase_analysis.main.log
```

**预期结果**：
- ✅ 能正常连接数据库
- ✅ 能从API获取店铺列表
- ✅ 数据能正常采集和存储

---

### 阶段四：配置定时任务 ⏰

#### 10. 配置Crontab

```bash
# 编辑定时任务
crontab -e

# 添加以下内容（根据需求调整时间）
# 每天凌晨2点执行采购分析项目
0 2 * * * cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.purchase_analysis.main >> /opt/apps/pythondata/logs/cron.log 2>&1

# 保存退出（vim操作：按ESC，输入:wq）

# 查看定时任务列表
crontab -l
```

#### 11. 验证定时任务

```bash
# 查看cron日志
tail -f /var/log/syslog | grep CRON  # Ubuntu/Debian
# 或
tail -f /var/log/cron  # CentOS

# 手动运行测试
/opt/apps/pythondata/venv/bin/python -m jobs.purchase_analysis.main

# 查看执行日志
tail -f /opt/apps/pythondata/logs/cron.log
```

---

### 阶段五：监控和维护 📊

#### 12. 日常监控

```bash
# 查看项目状态
cd /opt/apps/pythondata
bash scripts/check_status.sh

# 查看实时日志
tail -f logs/$(date +%Y-%m-%d)/purchase_analysis.main.log

# 查看磁盘空间
df -h

# 查看数据库数据
mysql -u your_user -p
USE lingxing;
SELECT COUNT(*) FROM lx_purchase_orders;
SELECT COUNT(*) FROM operation_orders;
SELECT COUNT(*) FROM order_analysis;
```

#### 13. 定期清理

```bash
# 清理30天前的日志
find /opt/apps/pythondata/logs -name "*.log" -mtime +30 -delete

# 或添加到crontab自动清理
0 1 * * 0 find /opt/apps/pythondata/logs -name "*.log" -mtime +30 -delete
```

---

## ✅ 完整上线检查清单

### 本地准备
- [ ] 代码测试通过
- [ ] Git提交代码
- [ ] 推送到远程仓库

### 服务器配置
- [ ] SSH登录服务器成功
- [ ] 克隆/更新代码完成
- [ ] 虚拟环境创建
- [ ] 依赖安装完成
- [ ] .env 配置完成
- [ ] 测试运行成功

### 定时任务
- [ ] Crontab配置完成
- [ ] 验证定时任务正常

### 监控维护
- [ ] 能查看日志
- [ ] 数据正常入库
- [ ] 配置日志清理

---

## 🆘 故障处理

### 如果出现问题：

1. **查看日志**：`logs/` 目录下的日志文件
2. **检查配置**：`.env` 文件是否正确
3. **测试连接**：数据库、API是否能连接
4. **查看文档**：
   - [INSTALL.md](INSTALL.md) - 安装问题
   - [FAQ.md](FAQ.md) - 常见问题
   - [DEPLOY.md](DEPLOY.md) - 部署详情

### 回滚方案

如果新版本有问题，可以回滚到上一版本：

```bash
# 查看提交历史
git log --oneline

# 回滚到上一个版本
git reset --hard HEAD^

# 或回滚到指定版本
git reset --hard <commit_hash>

# 重启服务
python -m jobs.purchase_analysis.main
```

---

## 📞 联系支持

如遇到无法解决的问题：
1. 查看日志文件
2. 运行 `bash scripts/check_status.sh`
3. 检查相关文档
4. 联系开发团队

---

**最后更新**: 2025-12-22

