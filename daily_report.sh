YEAR=$(date -d "$d" '+%Y')
MONTH=$(date -d "$d" '+%m')
DAY=$(date -d "$d" '+%d')
day_partition=$YEAR$MONTH$DAY
day_before=$(date -d "yesterday 13:00 " '+%Y%m%d')
bq_partitioned_table="shc-pricing-prod:daily_summary.daily_summary"'_'"${day_partition}"
bq query --destination_table=$bq_partitioned_table "

select 
'${day_partition}' run_date, a.run_id run_id, a.table_id table_id, a.row_count today_cnt, b.row_count yest_cnt
from 
(SELECT
  left(table_id,5) tbl_left , case when integer(RIGHT(table_id,2)) < 13 then 1
          when integer(RIGHT(table_id,2)) >= 13 and integer(RIGHT(table_id,2)) < 19 then 2
          when integer(RIGHT(table_id,2)) >= 19 then 3 end run_id,
   table_id, row_count
FROM
   [shc-pricing-prod:FTP_tables.__TABLES__]
WHERE
  REGEXP_MATCH(table_id, r'FTP')
  AND left(RIGHT(table_id, 10),8) = '${day_partition}')  a
left join 
(SELECT
  left(table_id,5) tbl_left , case when integer(RIGHT(table_id,2)) < 13 then 1
          when integer(RIGHT(table_id,2)) >= 13 and integer(RIGHT(table_id,2)) < 19 then 2
          when integer(RIGHT(table_id,2)) >= 19 then 3 end run_id,
   table_id, row_count 
FROM
  [shc-pricing-prod:FTP_tables.__TABLES__]
WHERE
  REGEXP_MATCH(table_id, r'FTP')
  AND left(RIGHT(table_id, 10),8) = '${day_before}') b
on a.tbl_left = b.tbl_left and a.run_id = b.run_id
order by run_id, table_id
  "
