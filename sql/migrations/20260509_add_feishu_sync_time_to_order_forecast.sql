-- 为运营预计下单表增加真实飞书同步时间字段。
-- 该字段由 jobs/feishu/write_order_forecast_to_feishu.py 在每次写入/更新时维护。

ALTER TABLE `运营预计下单表`
ADD COLUMN `飞书同步时间` DATETIME DEFAULT NULL COMMENT '本条记录最近一次从飞书同步入库时间';
