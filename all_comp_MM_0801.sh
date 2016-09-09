current_date=$(date +%Y-%m-%d)
day_partition=$(date +%Y%m%d)
HOUR=$(date '+%H')
hour_partition=$day_partition$HOUR
bq_partitioned_table="shc-pricing-prod:all_comp_tables.all_comp_MM"'_'"${hour_partition}"
bq query --destination_table=$bq_partitioned_table "
SELECT concat ,website_name, comp_name , shipping , price, a.div_no div_no, a.itm_no itm_no, min_margin,last_crawled_date 
FROM (select * from 
TABLE_QUERY([shc-pricing-prod:all_comp_tables], 
      'table_id CONTAINS \"all_comp_all\" 
      AND table_id= (Select MAX(table_id) 
                              FROM [shc-pricing-prod:all_comp_tables.__TABLES__]
                              where table_id contains \"all_comp_all\")')) a 
join  (select div_no, itm_no, min_margin from 
TABLE_QUERY([shc-pricing-prod:static_tables], 
      'table_id CONTAINS \"static_table_MM\" 
      AND table_id= (Select MAX(table_id) 
                              FROM [shc-pricing-prod:static_tables.__TABLES__]
                              where table_id contains \"static_table_MM\")'))  b 
on a.div_no = b.div_no and a.itm_no = b.itm_no 
where a.price > b.min_margin group by 1,2,3,4,5,6,7,8,9"

