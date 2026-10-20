"""Serial indexed missing-history extraction; all query guards are inherited."""
import argparse
import datetime as dt
from pathlib import Path
import subprocess
import sys

FIELDS = """dt,store_name,spu,SUM(volume) AS sales_qty,COUNT(*) AS source_rows,
SUM(volume IS NULL) AS null_qty,SUM(volume<0) AS negative_qty,SUM(stat_date<>dt) AS date_mismatch,
COUNT(DISTINCT sku) AS sku_count,COUNT(DISTINCT asin) AS asin_count,COUNT(DISTINCT country) AS country_count,
SUM(sessions_total) AS listing_sessions_sum,SUM(sessions_total IS NULL) AS null_sessions,
SUM(order_items) AS order_qty,SUM(spend) AS ad_spend,SUM(clicks) AS ad_clicks,
SUM(ad_order_quantity) AS ad_orders,SUM(ad_sales_amount) AS ad_sales_amt,SUM(amount) AS sales_amt,
SUM(delete_flag<>0) AS deleted_rows,MIN(create_time) AS min_created_at,MAX(modify_time) AS max_sync,
SUM(spend IS NULL) AS null_spend,SUM(clicks IS NULL) AS null_clicks,
SUM(ad_order_quantity IS NULL) AS null_ad_orders,SUM(order_items IS NULL) AS null_order_qty,
SUM(promotion_volume) AS promotion_units,SUM(promotion_volume IS NULL) AS null_promotion,
SUM(price*volume) AS volume_weighted_price_numerator,SUM(CASE WHEN price IS NOT NULL THEN volume END) AS priced_units,
SUM(afn_fulfillable_quantity IS NULL) AS unknown_inventory_rows,
SUM(afn_fulfillable_quantity=0) AS zero_inventory_observation_rows"""


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--root',required=True)
    ap.add_argument('--start',required=True);ap.add_argument('--end-exclusive',required=True)
    ap.add_argument('--prefix',default='daily_missing');a=ap.parse_args()
    root=Path(a.root).resolve(); sqlroot=root/'sql';sqlroot.mkdir(exist_ok=True)
    start=dt.date.fromisoformat(a.start);end=dt.date.fromisoformat(a.end_exclusive)
    if start<dt.date(2024,1,1) or end>dt.date(2026,10,2) or start>=end:raise ValueError('DATE_SCOPE')
    while start<end:
        stop=min(start+dt.timedelta(days=3),end)
        dates=[start+dt.timedelta(days=i) for i in range((stop-start).days)]
        name=a.prefix+'_'+str(start)+'_'+str(stop-dt.timedelta(days=1))
        if (root/'evidence'/(name+'.json')).exists():raise ValueError('ATTEMPT_EXISTS_NO_RETRY')
        literals=','.join("'"+str(x)+"'" for x in dates)
        sql=f"SELECT {FIELDS} FROM ods_db.ods_lx_product_performance WHERE dt IN ({literals}) AND store_name IN ('JQ-US','MT-US','RKZ-US','SY-US') GROUP BY dt,store_name,spu"
        sqlpath=sqlroot/(name+'.sql')
        with sqlpath.open('x',encoding='utf-8') as fh:fh.write(sql)
        cp=subprocess.run([sys.executable,'-B',str(Path(__file__).with_name('bc60_monthly_source.py')),
                           '--root',str(root),'--name',name,'--sql-file',str(sqlpath)],capture_output=True,text=True,encoding='utf-8')
        print(name,cp.stdout.strip(),flush=True)
        if cp.returncode:raise SystemExit('STOP_NEW_READS_QUERY_OR_BUDGET_FAILURE')
        start=stop


if __name__=='__main__':main()
