# 费用单管理API参数文档

本文档详细记录了费用单管理的完整流程（查询-作废-创建）中所有API调用的参数。

---

## 一、查询费用类型列表

### API端点
```
POST /bd/fee/management/open/feeManagement/otherFee/typeList
```

### 请求参数
```json
{}
```
**说明：** 此接口无需参数，直接发送空对象即可。

### 响应示例
```json
{
  "code": 0,
  "msg": "success",
  "data": [
    {
      "id": 123456,
      "name": "商品成本附加费",
      "sort": 1
    },
    {
      "id": 123457,
      "name": "头程成本附加费",
      "sort": 2
    },
    {
      "id": 123458,
      "name": "头程费用",
      "sort": 3
    },
    {
      "id": 123459,
      "name": "汇损",
      "sort": 4
    }
  ]
}
```

### 代码调用示例
```python
fee_types = await fee_mgmt.get_fee_types()
```

---

## 二、查询费用单列表

### API端点
```
POST /bd/fee/management/open/feeManagement/otherFee/list
```

### 请求参数

| 参数名 | 类型 | 必填 | 说明 | 示例值 |
|--------|------|------|------|--------|
| offset | int | 是 | 分页偏移量，从0开始 | 0 |
| length | int | 是 | 每页数量，建议20-100 | 20 |
| date_type | str | 是 | 时间类型：`gmt_create`(创建日期) 或 `date`(分摊日期) | "date" |
| start_date | str | 否 | 开始日期，格式：Y-m-d | "2026-01-01" |
| end_date | str | 否 | 结束日期，格式：Y-m-d | "2026-01-31" |
| sids | List[int] | 否 | 店铺ID列表 | [11545] |
| other_fee_type_ids | List[int] | 否 | 费用类型ID列表 | [123456, 123457] |
| status_order | int | 否 | 单据状态：1=待提交, 2=待审批, 3=已处理, 4=已驳回, 5=已作废 | 3 |
| dimensions | List[int] | 否 | 分摊维度：1=msku, 2=asin, 3=店铺, 4=父asin, 5=sku, 6=企业 | [1] |
| search_field | str | 否 | 搜索类型：number/msku/asin/create_name/remark_order/remark_item | null |
| search_value | str | 否 | 搜索值 | null |

### 实际调用示例（作废前查询）

```json
{
  "offset": 0,
  "length": 20,
  "date_type": "date",
  "start_date": "2026-01-01",
  "end_date": "2026-01-31",
  "other_fee_type_ids": [123456, 123457, 123458, 123459],
  "status_order": 3
}
```

**说明：**
- `date_type`: 使用 `"date"` 表示按分摊日期查询
- `status_order`: 使用 `3` 表示只查询"已处理"状态的费用单
- `other_fee_type_ids`: 包含四个费用类型的ID（商品成本附加费、头程成本附加费、头程费用、汇损）

### 响应示例
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "total": 100,
    "records": [
      {
        "number": "FE20260101001",
        "status_order": 3,
        "remark": "Auto-2026-01",
        "dimension": 1,
        "apportion_rule": 2,
        "create_time": "2026-01-01 10:00:00",
        "date": "2026-01"
      }
    ]
  }
}
```

### 代码调用示例
```python
query_result = await fee_mgmt.get_fee_list(
    offset=0,
    length=20,
    date_type="date",
    start_date="2026-01-01",
    end_date="2026-01-31",
    other_fee_type_ids=[123456, 123457, 123458, 123459],
    status_order=3
)
```

---

## 三、作废费用单

### API端点
```
POST /bd/fee/management/open/feeManagement/otherFee/discard
```

### 请求参数

| 参数名 | 类型 | 必填 | 说明 | 示例值 |
|--------|------|------|------|--------|
| numbers | List[str] | 是 | 费用单号列表，上限200个 | ["FE20260101001", "FE20260101002"] |

### 实际调用示例

```json
{
  "numbers": [
    "FE20260101001",
    "FE20260101002",
    "FE20260101003"
  ]
}
```

**说明：**
- 每次最多作废200个费用单
- 费用单号从查询接口的 `number` 字段获取

### 响应示例
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "success_count": 3,
    "failed_count": 0
  }
}
```

### 代码调用示例
```python
discard_result = await fee_mgmt.discard_fee_orders(
    numbers=["FE20260101001", "FE20260101002", "FE20260101003"]
)
```

---

## 四、创建费用单

### API端点
```
POST /bd/fee/management/open/feeManagement/otherFee/create
```

### 请求参数

#### 顶层参数

| 参数名 | 类型 | 必填 | 说明 | 示例值 |
|--------|------|------|------|--------|
| submit_type | int | 是 | 提交类型：1=暂存, 2=提交 | 2 |
| dimension | int | 是 | 分摊维度：1=msku, 2=asin, 3=店铺, 4=父asin, 5=sku, 6=企业 | 1 |
| apportion_rule | int | 是 | 分摊规则：0=无, 1=按销售额, 2=按销量, 3=店铺均摊后按销售额占比分摊, 4=店铺均摊后按销量占比分摊 | 2 |
| is_request_pool | int | 是 | 是否请款：0=否, 1=是 | 0 |
| remark | str | 是 | 费用单备注（建议使用英文，避免编码问题） | "Auto-2026-01-1" |
| fee_items | List[Dict] | 是 | 费用明细项列表，详见下方 | [...] |

#### fee_items 参数（费用明细项）

每个费用明细项包含以下字段：

| 参数名 | 类型 | 必填 | 说明 | 示例值 |
|--------|------|------|------|--------|
| sids | List[int] | 是 | 店铺ID列表 | [11545] |
| dimension_value | str | 是 | 维度值，例如MSKU值 | "LABX373-SN-XL-FBA-BS-SY014" |
| date | str | 是 | 分摊日期，格式：Y-m-d 或 Y-m（按月汇总时使用Y-m） | "2026-01" |
| other_fee_type_id | int | 是 | 费用类型ID（从查询费用类型列表获取） | 123456 |
| fee | float | 是 | 金额（原币金额，注意正负数） | -123.45 |
| currency_code | str | 是 | 币种代码 | "CNY" |
| remark | str | 是 | 费用子项备注（建议使用英文，避免编码问题） | "LABX373-SN-XL-FBA-BS-SY014-ProductCost" |

### 实际调用示例

#### 单个费用单（费用项数量 <= 50）

```json
{
  "submit_type": 2,
  "dimension": 1,
  "apportion_rule": 2,
  "is_request_pool": 0,
  "remark": "Auto-2026-01",
  "fee_items": [
    {
      "sids": [11545],
      "dimension_value": "LABX373-SN-XL-FBA-BS-SY014",
      "date": "2026-01",
      "other_fee_type_id": 123456,
      "fee": -123.45,
      "currency_code": "CNY",
      "remark": "LABX373-SN-XL-FBA-BS-SY014-ProductCost"
    },
    {
      "sids": [11545],
      "dimension_value": "LABX373-SN-XL-FBA-BS-SY014",
      "date": "2026-01",
      "other_fee_type_id": 123457,
      "fee": -45.67,
      "currency_code": "CNY",
      "remark": "LABX373-SN-XL-FBA-BS-SY014-InboundCost"
    }
  ]
}
```

#### 分批创建（费用项数量 > 50）

当费用项数量超过50时，需要分批创建，每批最多50项：

**第1批：**
```json
{
  "submit_type": 2,
  "dimension": 1,
  "apportion_rule": 2,
  "is_request_pool": 0,
  "remark": "Auto-2026-01-1",
  "fee_items": [
    // ... 最多50个费用项
  ]
}
```

**第2批：**
```json
{
  "submit_type": 2,
  "dimension": 1,
  "apportion_rule": 2,
  "is_request_pool": 0,
  "remark": "Auto-2026-01-2",
  "fee_items": [
    // ... 最多50个费用项
  ]
}
```

### 费用类型映射

| 费用类型名称 | 费用类型ID字段 | 备注格式 |
|-------------|---------------|---------|
| 商品成本附加费 | `商品成本附加费_id` | `{msku}-ProductCost` |
| 头程成本附加费 | `头程成本附加费_id` | `{msku}-InboundCost` |
| 头程费用 | `头程费用_id` | `{msku}-InboundFee` |
| 汇损 | `汇损_id` | `{msku}-ExchangeLoss` |

### 代码调用示例

```python
result = await fee_mgmt.create_fee_order(
    submit_type=2,  # 2=提交
    dimension=1,  # 1=msku
    apportion_rule=2,  # 2=按销量
    is_request_pool=0,  # 0=否
    remark="Auto-2026-01-1",
    fee_items=[
        {
            "sids": [11545],
            "dimension_value": "LABX373-SN-XL-FBA-BS-SY014",
            "date": "2026-01",
            "other_fee_type_id": 123456,
            "fee": -123.45,
            "currency_code": "CNY",
            "remark": "LABX373-SN-XL-FBA-BS-SY014-ProductCost"
        }
    ]
)
```

---

## 五、完整流程示例

### 1. 初始化Token
```python
fee_mgmt = FeeManagement()
await fee_mgmt.init_token()
```

### 2. 查询费用类型列表
```python
fee_types = await fee_mgmt.get_fee_types()
# 提取费用类型ID
fee_type_map = {ft.get('name'): ft.get('id') for ft in fee_types}
商品成本附加费_id = fee_type_map.get('商品成本附加费')
头程成本附加费_id = fee_type_map.get('头程成本附加费')
头程费用_id = fee_type_map.get('头程费用')
汇损_id = fee_type_map.get('汇损')
```

### 3. 查询需要作废的费用单
```python
query_result = await fee_mgmt.get_fee_list(
    offset=0,
    length=20,
    date_type="date",
    start_date="2026-01-01",
    end_date="2026-01-31",
    other_fee_type_ids=[商品成本附加费_id, 头程成本附加费_id, 头程费用_id, 汇损_id],
    status_order=3  # 只查询"已处理"状态
)

# 提取费用单号
records = query_result.get('data', {}).get('records', [])
numbers = [r.get('number') for r in records if r.get('number')]
```

### 4. 作废费用单
```python
# 分批作废，每批最多200个
for i in range(0, len(numbers), 200):
    batch_numbers = numbers[i:i + 200]
    await fee_mgmt.discard_fee_orders(batch_numbers)
    await asyncio.sleep(8)  # 等待8秒
```

### 5. 创建费用单
```python
# 构建费用明细项
fee_items = []
for record in profit_data:
    msku = record.get('MSKU')
    shop_id = record.get('店铺id')
    
    # 商品成本附加费
    if record.get('商品成本附加费', 0) != 0:
        fee_items.append({
            "sids": [int(shop_id)],
            "dimension_value": msku,
            "date": "2026-01",  # 年月格式
            "other_fee_type_id": 商品成本附加费_id,
            "fee": record.get('商品成本附加费'),
            "currency_code": "CNY",
            "remark": f"{msku}-ProductCost"
        })
    
    # ... 其他费用类型类似

# 创建费用单（如果超过50项，需要分批）
if len(fee_items) > 50:
    # 分批创建
    for i in range(0, len(fee_items), 50):
        batch_items = fee_items[i:i + 50]
        batch_num = i // 50 + 1
        await fee_mgmt.create_fee_order(
            submit_type=2,
            dimension=1,
            apportion_rule=2,
            is_request_pool=0,
            remark=f"Auto-2026-01-{batch_num}",
            fee_items=batch_items
        )
        await asyncio.sleep(8)  # 等待8秒
else:
    # 直接创建
    await fee_mgmt.create_fee_order(
        submit_type=2,
        dimension=1,
        apportion_rule=2,
        is_request_pool=0,
        remark="Auto-2026-01",
        fee_items=fee_items
    )
```

---

## 六、重要参数说明

### 1. 日期格式
- **查询接口**：使用 `Y-m-d` 格式，例如 `"2026-01-01"`
- **创建接口**：使用 `Y-m` 格式（按月汇总），例如 `"2026-01"`

### 2. 批次大小限制
- **查询批次**：建议每批20-100条
- **作废批次**：每批最多200个费用单号
- **创建批次**：每批最多50个费用项（如果格式化参数长度超过8000字符，会自动减小批次）

### 3. 请求延迟
- **查询后**：等待8秒
- **作废后**：等待8秒
- **创建后**：等待8秒
- **每20批后**：休息30秒（避免累积速率限制）

### 4. 备注格式（重要）
为避免编码问题导致签名错误，所有备注都使用英文：
- 费用单备注：`"Auto-{year_month}-{batch_num}"` 或 `"Auto-{year_month}"`
- 费用项备注：
  - `"{msku}-ProductCost"` （商品成本附加费）
  - `"{msku}-InboundCost"` （头程成本附加费）
  - `"{msku}-InboundFee"` （头程费用）
  - `"{msku}-ExchangeLoss"` （汇损）

### 5. 数据类型要求
- `sids`: 必须是 `List[int]`，不能是字符串
- `other_fee_type_id`: 必须是 `int`，不能是字符串
- `fee`: 必须是 `float` 或 `int`，支持负数
- `date`: 必须是字符串，格式为 `"Y-m"` 或 `"Y-m-d"`

### 6. 分摊规则说明
- `0`: 无分摊
- `1`: 按销售额分摊
- `2`: 按销量分摊（当前使用）
- `3`: 店铺均摊后按销售额占比分摊
- `4`: 店铺均摊后按销量占比分摊

---

## 七、错误处理

### 常见错误码
- `2001006`: api sign not correct（签名错误）
- `3001008`: 令牌桶无令牌（速率限制）
- `401/403`: Token过期或无效
- `2001003/2001005/3001001/3001002`: Token/签名相关错误

### 重试策略
1. **签名错误**：
   - 前2次：等待后重试（不刷新token，可能是速率限制）
   - 第3次：刷新token，等待10秒后重试
   - 后续：继续重试

2. **速率限制**：
   - 指数退避：等待时间 = 5秒 × 2^重试次数

3. **Token刷新**：
   - 优先使用 `refresh_token`
   - 如果失败，使用 `generate_access_token`
   - 如果token相同，接受该token（可能仍然有效）

---

## 八、完整参数示例（JSON格式）

### 查询费用单列表
```json
{
  "offset": 0,
  "length": 20,
  "date_type": "date",
  "start_date": "2026-01-01",
  "end_date": "2026-01-31",
  "other_fee_type_ids": [123456, 123457, 123458, 123459],
  "status_order": 3
}
```

### 作废费用单
```json
{
  "numbers": [
    "FE20260101001",
    "FE20260101002",
    "FE20260101003"
  ]
}
```

### 创建费用单（单批）
```json
{
  "submit_type": 2,
  "dimension": 1,
  "apportion_rule": 2,
  "is_request_pool": 0,
  "remark": "Auto-2026-01",
  "fee_items": [
    {
      "sids": [11545],
      "dimension_value": "LABX373-SN-XL-FBA-BS-SY014",
      "date": "2026-01",
      "other_fee_type_id": 123456,
      "fee": -123.45,
      "currency_code": "CNY",
      "remark": "LABX373-SN-XL-FBA-BS-SY014-ProductCost"
    },
    {
      "sids": [11545],
      "dimension_value": "LABX373-SN-XL-FBA-BS-SY014",
      "date": "2026-01",
      "other_fee_type_id": 123457,
      "fee": -45.67,
      "currency_code": "CNY",
      "remark": "LABX373-SN-XL-FBA-BS-SY014-InboundCost"
    },
    {
      "sids": [11545],
      "dimension_value": "LABX373-SN-XL-FBA-BS-SY014",
      "date": "2026-01",
      "other_fee_type_id": 123458,
      "fee": -89.12,
      "currency_code": "CNY",
      "remark": "LABX373-SN-XL-FBA-BS-SY014-InboundFee"
    },
    {
      "sids": [11545],
      "dimension_value": "LABX373-SN-XL-FBA-BS-SY014",
      "date": "2026-01",
      "other_fee_type_id": 123459,
      "fee": -12.34,
      "currency_code": "CNY",
      "remark": "LABX373-SN-XL-FBA-BS-SY014-ExchangeLoss"
    }
  ]
}
```

---

## 九、注意事项

1. **Token管理**：
   - 每次API调用前确保token有效
   - Token过期时自动刷新，无需手动处理

2. **批次处理**：
   - 费用项超过50个时自动分批
   - 格式化参数长度超过8000字符时自动减小批次

3. **速率限制**：
   - 每批请求后等待8秒
   - 每20批后休息30秒

4. **数据验证**：
   - 确保所有费用项的数据类型正确
   - 确保MSKU、店铺ID等关键字段不为空

5. **错误处理**：
   - 创建失败时立即停止，不跳过
   - 记录详细错误日志便于排查

---

**文档版本**: 1.0  
**最后更新**: 2026-02-07  
**维护者**: Auto-generated from create_fee_management.py

