#!/bin/bash
echo "Current Timestamp: `date`"
table_list='smith_sears_hierarchy_data 
 smith_sears_cost_data 
 smith_sears_subsidy_data
 smith_sears_exclusive_items
 smith_sears_block_items 
 smith_sears_ad_data 
 smith_sears_map_rules 
 smith_hub_online_price 
 smith_pmi_9300_data 
 smith_omniture_data 
 smith_apt_sears_sales 
 smith__marketrack_output_history 
 smith_pricing_hub_market_track_map 
 smith_ql2_daily_feed 
 incoming__onlineitems_price_w_hierarchy 
 incoming__dp_blocked_items'
tables=($table_list)
missingtbl=""
day_partition=$(date +%Y-%m-%d)
for table in ${tables[@]}
do
match=$(bq query "SELECT max(date(_PARTITIONTIME)) FROM [shc-pricing-prod:bq_pricing_it.$table] ")
oldIFS="$IFS"
IFS="|"
date_raw=($match)
IFS="$oldIFS"
max_date=${date_raw[3]}
if  [ "$max_date" != " $day_partition " ]
then 
foo=$
missingtbl=$missingtbl$table" ,"
fi      
done
echo ${missingtbl::-1}" do not exist" | mail -s "tables are missing!!" Lingwei.Shu@searshc.com
