#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""查询数据库中的店铺ID信息"""
import pymysql

MYSQL_CONFIG = {
    'host': 'rm-wz91237y91oasq45fco.mysql.rds.aliyuncs.com',
    'port': 3306,
    'user': 'SYSJ001',
    'password': 'Sysj20170413',
    'database': 'lingxing',
    'charset': 'utf8mb4'
}

def query_shop_ids_from_purchase_table():
    """从采购单表中统计所有店铺ID及其记录数"""
    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    sql = """
    SELECT 
        店铺名,
        COUNT(*) as 记录数
    FROM 采购单
    WHERE 店铺名 IS NOT NULL
    GROUP BY 店铺名
    ORDER BY 记录数 DESC
    """
    
    cursor.execute(sql)
    results = cursor.fetchall()
    
    print("\n" + "="*60)
    print("采购单表中的店铺统计")
    print("="*60)
    print(f"{'店铺名':<30} {'记录数':>10}")
    print("-"*60)
    
    for row in results:
        shop_name = row[0] if row[0] else '(空)'
        count = row[1]
        print(f"{shop_name:<30} {count:>10}")
    
    print("="*60)
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    query_shop_ids_from_purchase_table()

