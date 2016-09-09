current_date=$(date +%Y-%m-%d)
day_partition=$(date +%Y%m%d)
HOUR=$(date '+%H')
hour_partition=$day_partition$HOUR
bq_partitioned_table="shc-pricing-prod:min_comp_tables.min_comp_MM"'_'"${hour_partition}"
bq query --destination_table=$bq_partitioned_table "
select aaa.div_no div_no , aaa.itm_no itm_no ,bbb.min_comp min_comp_MM, aaa.min_comp_name  min_comp_MM_NM from 
(select div_no , itm_no , price , min_margin, min(comp_name) min_comp_name from 
(
SELECT *, rank() over (PARTITION BY concat order by last_crawled_date desc) rank_
FROM TABLE_QUERY([shc-pricing-prod:all_comp_tables], 
      'table_id CONTAINS \"all_comp_MM\" 
      AND table_id= (Select MAX(table_id) 
                              FROM [shc-pricing-prod:all_comp_tables.__TABLES__]
                              where table_id contains \"all_comp_MM\")')
where (
  div_no = 8 and LTRIM(RTRIM(website_name)) in ('36','11','20','22','134','94','6','3','141','25','2','4','23','68','5','84','12') or
  div_no in (9,34) and LTRIM(RTRIM(website_name)) in ('2','3') or  
  div_no in (26,46,22) and LTRIM(RTRIM(website_name)) in ('2','3','6') or   
  div_no =6 and LTRIM(RTRIM(website_name)) in ('14','110','64','12','5','4') or 
  div_no =52 and LTRIM(RTRIM(website_name)) in ('12','11','5','4') or 
  div_no =34 and LTRIM(RTRIM(website_name)) in ('2','3') or
  div_no =71 and LTRIM(RTRIM(website_name)) in ('2','3','12','5','4','141') or
  div_no =95 and LTRIM(RTRIM(website_name)) in ('COSTCO','DISCOUNT_TIRE','FIRESTONE','MAVISTIRE','MRTIRE','NATIONALTIREBATTERY','PEPBOYSTIRE','SAMS CLUB','WALMART') or  
  div_no = 28 and LTRIM(RTRIM(website_name)) not in ('WALMART')  
)							 
)
where rank_ = 1
group by 1,2,3,4) aaa
join
(select div_no , itm_no , min(price) min_comp from 
(
SELECT *, rank() over (PARTITION BY concat order by last_crawled_date desc) rank_
FROM TABLE_QUERY([shc-pricing-prod:all_comp_tables], 
      'table_id CONTAINS \"all_comp_MM\" 
      AND table_id= (Select MAX(table_id) 
                              FROM [shc-pricing-prod:all_comp_tables.__TABLES__]
                              where table_id contains \"all_comp_MM\")')
where (
  div_no = 8 and LTRIM(RTRIM(website_name)) in ('36','11','20','22','134','94','6','3','141','25','2','4','23','68','5','84','12') or
  div_no in (9,34) and LTRIM(RTRIM(website_name)) in ('2','3') or  
  div_no in (26,46,22) and LTRIM(RTRIM(website_name)) in ('2','3','6') or   
  div_no =6 and LTRIM(RTRIM(website_name)) in ('14','110','64','12','5','4') or 
  div_no =52 and LTRIM(RTRIM(website_name)) in ('12','11','5','4') or 
  div_no =34 and LTRIM(RTRIM(website_name)) in ('2','3') or
  div_no =71 and LTRIM(RTRIM(website_name)) in ('2','3','12','5','4','141') or
  div_no =95 and LTRIM(RTRIM(website_name)) in ('COSTCO','DISCOUNT_TIRE','FIRESTONE','MAVISTIRE','MRTIRE','NATIONALTIREBATTERY','PEPBOYSTIRE','SAMS CLUB','WALMART') or 
  div_no = 28 and LTRIM(RTRIM(website_name)) not in ('WALMART')  
)							  
)
where rank_ = 1
group by 1,2) bbb
on aaa.div_no = bbb.div_no and aaa.itm_no = bbb.itm_no and aaa.price = bbb.min_comp "

