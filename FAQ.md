# 常见问题解答 (FAQ)

## 📋 目录

- [环境配置问题](#环境配置问题)
- [数据库相关问题](#数据库相关问题)
- [API 调用问题](#api-调用问题)
- [服务器部署问题](#服务器部署问题)
- [定时任务问题](#定时任务问题)
- [数据采集问题](#数据采集问题)
- [性能优化问题](#性能优化问题)

---

## 环境配置问题

### Q1: 安装依赖时报错 "ERROR: Could not find a version that satisfies the requirement..."

**原因**：Python 版本过低或 pip 源访问受限

**解决方案**：

```bash
# 检查 Python 版本（需要 3.8+）
python --version

# 升级 pip
pip install --upgrade pip

# 使用国内镜像源
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 或配置永久镜像源
pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
```

### Q2: .env 文件不存在

**原因**：首次部署未配置环境变量

**解决方案**：

```bash
# 复制示例文件
cp env.example .env

# 编辑配置
vim .env

# 填入真实的配置信息：
# - 数据库连接信息
# - 灵星 API 密钥
# - 飞书 API 密钥
```

### Q3: 模块导入错误 "ModuleNotFoundError: No module named 'xxx'"

**原因**：未激活虚拟环境或依赖未安装

**解决方案**：

```bash
# 激活虚拟环境
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate     # Windows

# 安装依赖
pip install -r requirements.txt

# 或安装单个缺失的包
pip install package_name
```

### Q4: 虚拟环境创建失败

**原因**：系统未安装 python3-venv

**解决方案**：

```bash
# Ubuntu/Debian
sudo apt install python3-venv

# CentOS/RHEL
sudo yum install python3-venv

# 然后重新创建虚拟环境
python3 -m venv venv
```

---

## 数据库相关问题

### Q5: 数据库连接失败 "Can't connect to MySQL server"

**原因**：数据库配置错误或网络不通

**排查步骤**：

```bash
# 1. 检查 .env 配置
cat .env | grep DB_

# 2. 测试网络连通性
ping your_db_host

# 3. 测试数据库端口
telnet your_db_host 3306
# 或
nc -zv your_db_host 3306

# 4. 使用命令行测试连接
mysql -h your_db_host -P 3306 -u your_user -p

# 5. 检查防火墙规则
sudo iptables -L | grep 3306

# 6. 检查 MySQL 服务状态
sudo systemctl status mysql
```

**常见解决方案**：

```bash
# 修改 MySQL 配置允许远程连接
# 编辑 /etc/mysql/mysql.conf.d/mysqld.cnf
bind-address = 0.0.0.0  # 允许所有 IP 连接

# 授权远程访问
mysql -u root -p
GRANT ALL PRIVILEGES ON lingxing.* TO 'your_user'@'%' IDENTIFIED BY 'your_password';
FLUSH PRIVILEGES;

# 重启 MySQL
sudo systemctl restart mysql
```

### Q6: 数据库表不存在

**原因**：未创建表或数据库

**解决方案**：

```bash
# 手动创建表（参考各个 job 文件中的建表语句）
mysql -h your_db_host -u your_user -p lingxing

# 或在 Python 中执行建表
from jobs.purchase_order import PurchaseOrderCollector
collector = PurchaseOrderCollector()
collector.create_table()  # 如果有这个方法
```

### Q7: 中文数据乱码

**原因**：数据库字符集配置问题

**解决方案**：

```sql
-- 修改数据库字符集
ALTER DATABASE lingxing CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 修改表字符集
ALTER TABLE table_name CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 检查字符集
SHOW VARIABLES LIKE 'character_set%';
```

---

## API 调用问题

### Q8: 灵星 API 调用超时

**原因**：网络问题或需要配置代理

**解决方案**：

```bash
# 在 .env 中配置代理
LINGXING_PROXY_URL=http://username:password@proxy.example.com:1080

# 或使用系统代理
export HTTP_PROXY=http://proxy.example.com:1080
export HTTPS_PROXY=http://proxy.example.com:1080

# 测试代理连接
curl -x http://proxy.example.com:1080 https://openapi.lingxing.com
```

### Q9: API 返回 401 或 403 错误

**原因**：API 密钥错误或已过期

**排查步骤**：

```bash
# 1. 检查 API 配置
python -c "from common.config import settings; print(settings.LINGXING_APP_ID)"

# 2. 验证 API 密钥
# 登录灵星后台检查 APP_ID 和 APP_SECRET 是否正确

# 3. 检查 API 权限
# 确认应用是否有访问相关数据的权限
```

### Q10: 飞书 API 调用失败

**原因**：应用权限不足或 token 配置错误

**解决方案**：

```bash
# 1. 检查飞书配置
python -c "
from common.config import settings
print(f'APP_ID: {settings.FEISHU_APP_ID}')
print(f'APP_SECRET: {settings.FEISHU_APP_SECRET}')
"

# 2. 在飞书开放平台检查：
# - 应用权限（需要多维表格读写权限）
# - App Token 是否正确
# - 应用是否已启用

# 3. 测试获取 Access Token
python -c "
from common.feishu import FeishuAPI
api = FeishuAPI()
token = api.get_access_token()
print(f'Token: {token}')
"
```

---

## 服务器部署问题

### Q11: SSH 连接服务器失败

**原因**：防火墙、密钥或网络问题

**解决方案**：

```bash
# 使用详细模式查看错误
ssh -vvv user@server_ip

# 检查密钥权限
chmod 600 ~/.ssh/id_rsa
chmod 700 ~/.ssh

# 使用密码登录（如果密钥失败）
ssh -o PreferredAuthentications=password user@server_ip

# 检查服务器端 SSH 配置
# /etc/ssh/sshd_config
PermitRootLogin yes
PubkeyAuthentication yes
PasswordAuthentication yes
```

### Q12: 服务器磁盘空间不足

**排查和清理**：

```bash
# 检查磁盘使用情况
df -h

# 查找大文件
du -sh /* | sort -hr | head -10
du -sh /opt/apps/pythondata/* | sort -hr

# 清理日志文件
find /opt/apps/pythondata/logs -name "*.log" -mtime +30 -delete

# 清理 Python 缓存
find /opt/apps/pythondata -type d -name "__pycache__" -exec rm -rf {} +

# 清理 pip 缓存
pip cache purge

# 查看日志目录大小
du -sh /opt/apps/pythondata/logs
```

### Q13: 服务器性能问题

**排查步骤**：

```bash
# 查看 CPU 使用率
top
htop  # 更友好的界面

# 查看内存使用
free -h

# 查看进程
ps aux | grep python

# 查看磁盘 IO
iostat -x 1

# 查看网络连接
netstat -tunlp
ss -tunlp
```

---

## 定时任务问题

### Q14: Crontab 任务不执行

**原因**：多种可能

**排查步骤**：

```bash
# 1. 确认 cron 服务运行中
sudo systemctl status cron    # Ubuntu/Debian
sudo systemctl status crond   # CentOS

# 2. 检查 crontab 语法
crontab -l

# 3. 查看 cron 日志
sudo tail -f /var/log/syslog | grep CRON    # Ubuntu
sudo tail -f /var/log/cron                  # CentOS

# 4. 手动执行命令测试
cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.purchase_order

# 5. 检查脚本权限
ls -l scripts/*.sh

# 6. 检查路径是否正确（使用绝对路径）
which python
pwd
```

**常见问题解决**：

```bash
# 问题：环境变量不生效
# 解决：在 crontab 中设置环境变量
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
0 2 * * * cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.purchase_order

# 问题：相对路径错误
# 解决：使用绝对路径
0 2 * * * cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.purchase_order >> /opt/apps/pythondata/logs/cron.log 2>&1

# 问题：权限不足
# 解决：检查文件权限和所有者
sudo chown -R pythondata:pythondata /opt/apps/pythondata
chmod +x scripts/*.sh
```

### Q15: Crontab 日志看不到输出

**原因**：未配置日志重定向

**解决方案**：

```bash
# 方式一：重定向到日志文件
0 2 * * * /path/to/script.sh >> /path/to/log.log 2>&1

# 方式二：分别记录标准输出和错误输出
0 2 * * * /path/to/script.sh >> /path/to/output.log 2>> /path/to/error.log

# 方式三：配置邮件通知
MAILTO=your-email@example.com
0 2 * * * /path/to/script.sh

# 测试邮件配置
echo "Test" | mail -s "Test Subject" your-email@example.com
```

---

## 数据采集问题

### Q16: 数据重复采集

**原因**：未正确处理去重逻辑

**解决方案**：

```python
# 在数据库表中添加唯一索引
ALTER TABLE purchase_orders 
ADD UNIQUE KEY unique_order (po_no, sku, warehouse, created_time);

# 在插入时使用 INSERT IGNORE 或 ON DUPLICATE KEY UPDATE
INSERT IGNORE INTO purchase_orders (...) VALUES (...);

# 或使用 REPLACE INTO
REPLACE INTO purchase_orders (...) VALUES (...);
```

### Q17: 数据采集速度慢

**优化方案**：

```python
# 1. 使用批量插入而不是逐条插入
# 不好的方式：
for item in items:
    db.insert(item)

# 好的方式：
db.batch_insert(items, batch_size=100)

# 2. 减少数据库查询次数
# 使用 JOIN 而不是多次查询

# 3. 添加适当的索引
CREATE INDEX idx_sku ON purchase_orders(sku);
CREATE INDEX idx_created_time ON purchase_orders(created_time);

# 4. 使用异步 IO
# 参考 asyncio 和 aiohttp 的使用
```

### Q18: API 请求被限流

**原因**：请求过于频繁

**解决方案**：

```bash
# 在 .env 中增加延迟
COLLECTION_DELAY_SECONDS=2

# 或在代码中添加限流逻辑
import time
from functools import wraps

def rate_limit(seconds):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            time.sleep(seconds)
            return func(*args, **kwargs)
        return wrapper
    return decorator

@rate_limit(1)  # 每次请求间隔 1 秒
def fetch_data():
    pass
```

---

## 性能优化问题

### Q19: 内存占用过高

**排查和优化**：

```bash
# 查看内存占用
ps aux --sort=-%mem | head -10

# Python 内存分析
pip install memory_profiler
python -m memory_profiler your_script.py

# 优化建议：
# 1. 使用生成器而不是列表
# 2. 及时释放大对象
# 3. 使用批量处理
# 4. 避免在循环中创建大量对象
```

```python
# 不好的方式：
data = [process(item) for item in huge_list]

# 好的方式：
def process_items():
    for item in huge_list:
        yield process(item)

for result in process_items():
    save(result)
```

### Q20: 数据库查询慢

**优化步骤**：

```sql
-- 1. 分析慢查询
SHOW FULL PROCESSLIST;

-- 2. 查看查询执行计划
EXPLAIN SELECT * FROM purchase_orders WHERE sku = 'XXX';

-- 3. 添加索引
CREATE INDEX idx_sku ON purchase_orders(sku);
CREATE INDEX idx_created_time ON purchase_orders(created_time);

-- 4. 优化查询
-- 避免 SELECT *，只选择需要的字段
SELECT sku, quantity FROM purchase_orders WHERE ...

-- 5. 使用分页
SELECT * FROM purchase_orders LIMIT 1000 OFFSET 0;

-- 6. 定期优化表
OPTIMIZE TABLE purchase_orders;
```

---

## 🔧 调试技巧

### 查看详细日志

```bash
# 查看实时日志
tail -f logs/$(date +%Y-%m-%d)/purchase_order.log

# 搜索错误信息
grep -r "ERROR" logs/

# 查看最近的错误
tail -100 logs/$(date +%Y-%m-%d)/*.log | grep "ERROR"
```

### Python 调试

```python
# 添加调试日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 使用 pdb 调试
import pdb
pdb.set_trace()

# 打印变量
print(f"Debug: {variable}")
```

### 测试单个模块

```bash
# 测试配置加载
python -c "from common.config import settings; print(settings.db_config)"

# 测试数据库连接
python -c "from common.database import DatabaseManager; DatabaseManager().test_connection()"

# 测试 API 调用
python -c "from lingxing.openapi import OpenAPI; api = OpenAPI(); print(api.test())"
```

---

## 📞 获取帮助

如果以上解决方案都无法解决您的问题：

1. **检查日志文件**：`logs/` 目录下的详细日志
2. **查看项目文档**：README.md, DEPLOY.md, GIT_WORKFLOW.md
3. **运行状态检查**：`bash scripts/check_status.sh`
4. **联系开发团队**：提供详细的错误信息和日志

---

## 💡 预防问题的最佳实践

1. ✅ **定期备份数据库**
2. ✅ **监控磁盘空间**
3. ✅ **定期查看日志**
4. ✅ **测试后再部署**
5. ✅ **使用版本控制**
6. ✅ **保持依赖更新**
7. ✅ **配置告警通知**
8. ✅ **编写文档记录问题**

