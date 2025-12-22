# Git 工作流程说明

## 📋 概述

本文档描述了从本地开发到阿里云服务器部署的完整 Git 工作流程。

## 🔄 标准工作流程

### 1. 本地开发

```bash
# 确保在正确的分支
git branch

# 查看当前状态
git status

# 添加修改的文件
git add .

# 或者添加特定文件
git add jobs/new_job.py
git add common/config.py

# 提交更改（使用有意义的提交信息）
git commit -m "feat: 添加新的数据采集任务"

# 查看提交历史
git log --oneline -5
```

### 2. 推送到远程仓库

```bash
# 推送到远程仓库的 main 分支（或 master）
git push origin main

# 如果是第一次推送新分支
git push -u origin main

# 查看远程仓库信息
git remote -v
```

### 3. 服务器端更新

#### 方式一：手动更新（推荐用于测试）

```bash
# SSH 登录到阿里云服务器
ssh user@your-server-ip

# 切换到项目目录
cd /opt/apps/pythondata

# 拉取最新代码
git pull origin main

# 激活虚拟环境
source venv/bin/activate

# 更新依赖（如果 requirements.txt 有变化）
pip install -r requirements.txt --upgrade

# 测试运行
python -m jobs.purchase_order
```

#### 方式二：使用更新脚本（推荐用于生产）

```bash
# SSH 登录到服务器
ssh user@your-server-ip

# 切换到项目目录并执行更新脚本
cd /opt/apps/pythondata
bash scripts/update_project.sh
```

## 📝 提交信息规范

使用语义化的提交信息，便于理解和追踪：

### 提交类型

- `feat`: 新功能
- `fix`: 修复 Bug
- `docs`: 文档更新
- `style`: 代码格式调整（不影响功能）
- `refactor`: 代码重构
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建/工具链相关

### 示例

```bash
# 新功能
git commit -m "feat: 添加商品库存数据采集功能"
git commit -m "feat(jobs): 新增销售报表生成任务"

# 修复 Bug
git commit -m "fix: 修复采购单重复采集的问题"
git commit -m "fix(database): 解决连接池超时问题"

# 文档更新
git commit -m "docs: 更新部署文档"
git commit -m "docs(README): 添加 API 配置说明"

# 重构
git commit -m "refactor: 优化数据库查询逻辑"

# 性能优化
git commit -m "perf: 使用批量插入提升写入速度"

# 配置和工具
git commit -m "chore: 更新依赖包版本"
git commit -m "chore: 添加部署脚本"
```

## 🌿 分支管理

### 推荐的分支策略

```
main (或 master)     # 生产环境，稳定版本
└── develop          # 开发环境，最新功能
    ├── feature/xxx  # 功能分支
    └── hotfix/xxx   # 紧急修复分支
```

### 使用功能分支

```bash
# 从 develop 创建功能分支
git checkout develop
git pull origin develop
git checkout -b feature/new-data-collection

# 开发并提交
git add .
git commit -m "feat: 实现新的数据采集功能"

# 推送功能分支
git push origin feature/new-data-collection

# 开发完成后，合并到 develop
git checkout develop
git merge feature/new-data-collection

# 推送 develop
git push origin develop

# 测试无误后，合并到 main
git checkout main
git merge develop
git push origin main

# 删除功能分支（可选）
git branch -d feature/new-data-collection
git push origin --delete feature/new-data-collection
```

### 简化版本（小团队）

```bash
# 直接在 main 分支开发（不推荐，但适合单人或小团队）
git checkout main
git pull origin main

# 开发、提交、推送
git add .
git commit -m "feat: 添加新功能"
git push origin main

# 服务器更新
ssh user@server
cd /opt/apps/pythondata && bash scripts/update_project.sh
```

## 🔍 常用 Git 命令

### 查看状态和历史

```bash
# 查看当前状态
git status

# 查看修改内容
git diff

# 查看已暂存的修改
git diff --staged

# 查看提交历史
git log --oneline --graph --all -10

# 查看某个文件的修改历史
git log -p jobs/purchase_order.py
```

### 撤销和回退

```bash
# 撤销工作区的修改（未 add）
git checkout -- jobs/purchase_order.py

# 撤销暂存区的文件（已 add，未 commit）
git reset HEAD jobs/purchase_order.py

# 修改最后一次提交信息
git commit --amend -m "新的提交信息"

# 回退到上一个提交（保留修改）
git reset --soft HEAD^

# 回退到上一个提交（不保留修改，危险！）
git reset --hard HEAD^

# 回退到指定提交
git reset --hard commit_hash
```

### 处理冲突

```bash
# 拉取时发生冲突
git pull origin main
# Auto-merging file.py
# CONFLICT (content): Merge conflict in file.py

# 查看冲突文件
git status

# 手动编辑冲突文件，解决冲突标记
# <<<<<<< HEAD
# 本地内容
# =======
# 远程内容
# >>>>>>> commit_hash

# 解决后，标记为已解决
git add file.py

# 完成合并
git commit -m "merge: 解决冲突"
```

## 🚫 .gitignore 最佳实践

项目已配置 `.gitignore`，以下内容不会被提交：

- ✅ `.env` - 环境变量（包含敏感信息）
- ✅ `logs/` - 日志文件
- ✅ `__pycache__/` - Python 缓存
- ✅ `venv/` - 虚拟环境
- ✅ `*.pyc` - 编译文件

### 验证 .gitignore

```bash
# 查看哪些文件被忽略
git status --ignored

# 强制添加被忽略的文件（谨慎使用）
git add -f logs/important.log

# 检查某个文件是否被忽略
git check-ignore -v .env
```

## 🔐 安全注意事项

### 1. 永远不要提交敏感信息

```bash
# ❌ 错误：直接提交 .env
git add .env
git commit -m "添加配置"

# ✅ 正确：使用示例文件
git add env.example
git commit -m "添加配置示例"
```

### 2. 如果不小心提交了敏感信息

```bash
# 从历史中完全删除文件（谨慎使用）
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch .env" \
  --prune-empty --tag-name-filter cat -- --all

# 强制推送（会重写历史）
git push origin --force --all

# 更简单的方法：使用 git-filter-repo
pip install git-filter-repo
git filter-repo --path .env --invert-paths

# 推荐：如果刚提交未推送，使用 reset
git reset --soft HEAD^
# 移除敏感文件
git reset HEAD .env
# 添加到 .gitignore
echo ".env" >> .gitignore
# 重新提交
git commit -m "添加配置（移除敏感信息）"
```

### 3. 使用环境变量示例文件

```bash
# 提交示例配置
cp .env env.example
# 编辑 env.example，将真实值替换为占位符
vim env.example
git add env.example
git commit -m "docs: 添加环境变量配置示例"
```

## 🔄 服务器端 Git 配置

### 首次配置

```bash
# SSH 到服务器
ssh user@your-server-ip

# 配置 Git 用户信息
git config --global user.name "Your Name"
git config --global user.email "your@email.com"

# 配置凭据缓存（避免每次输入密码）
git config --global credential.helper cache
git config --global credential.helper 'cache --timeout=3600'

# 或使用 SSH 密钥（推荐）
ssh-keygen -t ed25519 -C "your@email.com"
cat ~/.ssh/id_ed25519.pub
# 将公钥添加到 Git 仓库的 Deploy Keys 或 SSH Keys
```

### 使用 SSH 克隆（推荐）

```bash
# 使用 SSH URL 克隆
git clone git@github.com:username/pythondata.git

# 而不是 HTTPS
# git clone https://github.com/username/pythondata.git
```

### 配置只读部署

```bash
# 在服务器上禁用推送（防止意外修改）
cd /opt/apps/pythondata
git config --local receive.denyCurrentBranch refuse

# 或者设置为只读仓库
git config --local core.fileMode false
```

## 📊 自动化部署

### 方式一：定时自动更新（简单）

```bash
# 添加到 crontab
crontab -e

# 每天凌晨 1:00 自动拉取最新代码
0 1 * * * cd /opt/apps/pythondata && git pull origin main >> logs/git_pull.log 2>&1
```

### 方式二：Git Hooks（高级）

在本地仓库配置 post-receive hook：

```bash
# 在服务器上创建裸仓库
mkdir -p /opt/git/pythondata.git
cd /opt/git/pythondata.git
git init --bare

# 创建 post-receive hook
cat > hooks/post-receive << 'EOF'
#!/bin/bash
GIT_WORK_TREE=/opt/apps/pythondata
GIT_DIR=/opt/git/pythondata.git
export GIT_WORK_TREE GIT_DIR

git checkout -f main

cd /opt/apps/pythondata
source venv/bin/activate
pip install -r requirements.txt --upgrade

echo "代码已更新: $(date)"
EOF

chmod +x hooks/post-receive

# 在本地添加服务器为远程仓库
git remote add production user@server:/opt/git/pythondata.git

# 推送到生产环境
git push production main
```

### 方式三：使用 CI/CD（专业）

配置 GitHub Actions / GitLab CI：

```yaml
# .github/workflows/deploy.yml
name: Deploy to Aliyun

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Deploy to Server
        uses: appleboy/ssh-action@master
        with:
          host: ${{ secrets.SERVER_HOST }}
          username: ${{ secrets.SERVER_USER }}
          key: ${{ secrets.SSH_PRIVATE_KEY }}
          script: |
            cd /opt/apps/pythondata
            bash scripts/update_project.sh
```

## 🐛 常见问题

### 1. 拉取时提示有本地修改

```bash
# 查看修改
git status

# 方案一：暂存本地修改
git stash
git pull origin main
git stash pop

# 方案二：放弃本地修改（危险！）
git reset --hard HEAD
git pull origin main
```

### 2. 推送被拒绝

```bash
# 错误：Updates were rejected because the remote contains work
# 原因：远程有新的提交

# 解决：先拉取再推送
git pull origin main
git push origin main

# 或使用 rebase
git pull --rebase origin main
git push origin main
```

### 3. 合并冲突

```bash
# 查看冲突文件
git status

# 编辑文件解决冲突
vim conflicted_file.py

# 标记为已解决
git add conflicted_file.py
git commit -m "merge: 解决冲突"
```

## 📚 参考资料

- [Git 官方文档](https://git-scm.com/doc)
- [Pro Git 中文版](https://git-scm.com/book/zh/v2)
- [Git 工作流程](https://www.atlassian.com/git/tutorials/comparing-workflows)

---

## 💡 最佳实践总结

1. ✅ **频繁提交**：小步提交，便于回滚和追踪
2. ✅ **有意义的提交信息**：使用语义化的提交信息
3. ✅ **测试后再推送**：确保代码可运行
4. ✅ **使用 .gitignore**：不提交敏感信息和临时文件
5. ✅ **定期同步**：经常 pull 和 push，避免大量冲突
6. ✅ **使用分支**：重大功能开发使用独立分支
7. ✅ **备份重要数据**：定期备份服务器数据库
8. ✅ **文档先行**：修改配置时同步更新文档

---

**记住**：Git 是版本控制工具，不是备份工具。重要数据应该有独立的备份策略！

