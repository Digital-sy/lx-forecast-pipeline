# 采购下单分析项目

## 📋 项目说明

本项目用于分析采购单与运营下单计划的差异。

### 功能模块

1. **fetch_purchase.py** - 从灵星API采集采购单数据
2. **fetch_operation.py** - 从飞书多维表格采集运营下单计划
3. **generate_analysis.py** - 生成采购vs计划的分析表
4. **shop_mapping.py** - 店铺映射管理（新增）
5. **main.py** - 主入口，按顺序执行所有任务

## 🏪 店铺映射功能

### 功能说明

`shop_mapping.py` 模块负责管理店铺ID到店铺名称的映射关系。

#### 映射来源

1. **固定映射（优先级最高）**：
   ```python
   '110521897148377600' → 'TK本土店-1店'
   '122513373670998016' → 'RR-EU'
   '110521891393331200' → 'TK跨境店-2店'
   ```

2. **API动态获取**：
   - 接口：`/erp/sc/data/seller/lists`
   - 自动获取企业已授权到领星ERP的所有亚马逊店铺
   - 通常可获取 15-20 个店铺（根据实际授权情况）

3. **RR开头统一规则**：
   - 所有 `RR-` 或 `RR_` 开头的店铺名称统一显示为 `RR-EU`
   - 示例：`RR-UK`, `RR-FR`, `RR_DE` 等都显示为 `RR-EU`

#### 实际效果

系统会自动合并固定映射和API映射，通常得到 **20+ 个店铺映射**：
- 固定映射：3 个（TK本土店-1店、TK跨境店-2店、RR-EU）
- API映射：17 个（JQ-US、MT-US、SY-US、多个RR店铺等）
- RR统一：6 个RR开头的店铺 → 统一为 RR-EU

### 使用方式

#### 方式一：在主程序中自动加载（推荐）

```python
# main.py 中已自动加载
from .shop_mapping import get_shop_mapping
shop_mapping = await get_shop_mapping()
```

#### 方式二：在各个任务中单独使用

```python
from .shop_mapping import get_shop_mapping, normalize_shop_id

# 获取完整映射
shop_mapping = await get_shop_mapping()

# 规范化店铺ID或名称
shop_name = normalize_shop_id('110521897148377600', shop_mapping)
# 返回: 'TK本土店-1店'

shop_name = normalize_shop_id('RR-US', shop_mapping)
# 返回: 'RR-EU'

shop_name = normalize_shop_id('RR_UK', shop_mapping)
# 返回: 'RR-EU'
```

#### 方式三：只使用固定映射（不调用API）

```python
from .shop_mapping import get_fixed_shop_mapping, normalize_shop_id

# 只获取固定映射（不调用API，更快）
shop_mapping = get_fixed_shop_mapping()

shop_name = normalize_shop_id(shop_id, shop_mapping)
```

### 添加新的固定映射

如果需要添加新的店铺映射，编辑 `shop_mapping.py` 中的 `FIXED_SHOP_MAPPINGS`：

```python
FIXED_SHOP_MAPPINGS = {
    '110521897148377600': 'TK本土店-1店',
    '122513373670998016': 'RR-EU',
    '110521891393331200': 'TK跨境店-2店',
    '新店铺ID': '新店铺名称',  # 添加新映射
}
```

### 测试店铺映射

```bash
# 运行测试代码
python -m jobs.purchase_analysis.shop_mapping
```

输出示例：
```
获取到 3 个店铺映射:
  110521897148377600 → TK本土店-1店
  110521891393331200 → TK跨境店-2店
  122513373670998016 → RR-EU

测试店铺ID规范化:
  110521897148377600 → TK本土店-1店
  122513373670998016 → RR-EU
  RR-US → RR-EU
  RR_UK → RR-EU
  JQ-US → JQ-US
  999999 → 999999
```

## ▶️ 运行方式

### 运行整个项目

```bash
python -m jobs.purchase_analysis.main
```

### 运行单个任务

```bash
# 只采集采购单
python -m jobs.purchase_analysis.fetch_purchase

# 只采集运营计划
python -m jobs.purchase_analysis.fetch_operation

# 只生成分析表
python -m jobs.purchase_analysis.generate_analysis

# 测试店铺映射
python -m jobs.purchase_analysis.shop_mapping
```

## 📊 数据表结构

### 采购单表 (lx_purchase_orders)
- 采购单号、SKU、店铺名、仓库、数量等

### 运营下单表 (operation_orders)
- SKU、店铺、下单数量、下单人、下单时间等

### 分析表 (order_analysis)
- SKU、店铺、月份
- 实际已下单、预计下单、下单差值

## 🔄 定时任务

在服务器上配置定时任务：

```bash
# 每天凌晨2点执行
0 2 * * * cd /opt/apps/pythondata && /opt/apps/pythondata/venv/bin/python -m jobs.purchase_analysis.main >> /opt/apps/pythondata/logs/cron_purchase_analysis.log 2>&1
```

## 📝 日志

日志文件位置：`logs/YYYY-MM-DD/purchase_analysis.*.log`

```bash
# 查看主程序日志
tail -f logs/$(date +%Y-%m-%d)/purchase_analysis.main.log

# 查看店铺映射日志
tail -f logs/$(date +%Y-%m-%d)/purchase_analysis.shop_mapping.log
```

## 🐛 常见问题

### Q: 为什么有些店铺显示的是ID而不是名称？

**A**: 可能是因为该店铺ID不在固定映射中，且API未返回该店铺信息。请在 `shop_mapping.py` 的 `FIXED_SHOP_MAPPINGS` 中添加映射。

### Q: 如何确认RR开头的店铺是否都统一为RR-EU了？

**A**: 查看分析表的输出统计，或运行：
```bash
python -m jobs.purchase_analysis.shop_mapping
```

### Q: 店铺映射加载失败怎么办？

**A**: 系统会自动降级使用固定映射，不影响程序运行。检查网络和API配置。

---

**最后更新**: 2025-12-22

