#!/bin/bash
day_partition=$(date +%Y%m%d)
HOUR=$(date '+%H')
hour_now=$day_partition$HOUR
hour_before="$(date -d "${hour_now:0:8} ${hour_now:8:2}:00:00 -2hour" '+%Y%m%d%H')"
echo ${hour_now}
echo ${hour_before}
all_divs='Div6 Div8 Div95 Div71 Div28 Div34 Div9 Div52'
div_arr=($all_divs)
for div in "${div_arr[@]}"
do
tbl_id_raw=$(bq query "SELECT table_id FROM [shc-pricing-prod:rule_tables.__TABLES__] where REGEXP_MATCH(table_id, r'^$div')  AND RIGHT(table_id, 10) BETWEEN '${hour_before}' AND '${hour_now}' ")
oldIFS="$IFS"
IFS="|"
tbl_id=( $tbl_id_raw )
IFS="$oldIFS"
tbls=""
len=${#tbl_id[@]}
last=$((len-2))	
for i in $(seq 3 2 $last)
do
tbl_nm=${tbl_id[$i]}
foo="[shc-pricing-prod:rule_tables.${tbl_nm//[[:space:]]/}],"
tbls=$tbls$foo
done
echo ${tbls::-1}
bq_partitioned_table="shc-pricing-prod:FTP_tables.$div"'_FTP_'"${hour_now}"
bq query  --destination_table=$bq_partitioned_table "SELECT
  'Sears.com' Format,
  '9300' Store,
  CONCAT(STRING(a_div_no),'-',STRING(a_itm_no)) Div_item,
  first(ROUND(Recom_prc,2)) Price,
  CURRENT_DATE() Start_Date,
  date(DATE_ADD(CURRENT_DATE(),1,'DAY')) End_date,
  NULL Member_Flag,
  NULL Record_Type,
  NULL Region,
  first(a_online_exclusive) Online_Only,
  'N' DP_Block,
  first(Rule_Level) Apply_Deal_Flag
FROM(select * from ${tbls::-1} where Recom_prc is not null order by a_div_no, a_itm_no, Rule_Level desc)
group by Div_item"
done
