current_date=$(date +%Y-%m-%d)
day_partition=$(date +%Y%m%d)
HOUR=$(date '+%H')
hour_partition=$day_partition$HOUR
bq_partitioned_table="shc-pricing-prod:rule_tables.Div95_Automotive_5pct"'_'"${hour_partition}"
bq query --destination_table=$bq_partitioned_table "
select *, round((Recom_prc - a.cost_with_subsidy)/Recom_prc,2) pushed_MR from 
(select a.*, b.min_comp, b.min_comp_NM, c.min_comp_MM, c.min_comp_MM_NM,
(case when a.blocked is null and b.min_comp is not null and (a.ad_plan <> 'Y' or a.ad_plan is null)
then (case when c.min_comp_MM is null 
      then a.min_margin 
      else (case when c.min_comp_MM <> b.min_comp 
            then a.min_margin
            else c.min_comp_MM * 0.95
            end)
      end)
end) Recom_prc,
(case when a.blocked is null and b.min_comp is not null and (a.ad_plan <> 'Y' or a.ad_plan is null)
then (case when c.min_comp_MM is null 
      then 'Price at Min Margin Price'
      else (case when c.min_comp_MM <> b.min_comp 
            then 'Price at min margin when there is a competitor below min margin'
            else 'Min Comp MM -5%'
            end)
      end)
end) Rule_name,
'5-Item' Rule_Level
from 
(select * from 
TABLE_QUERY([shc-pricing-prod:static_tables], 
      'table_id CONTAINS \"static_table_MM\" 
      AND table_id= (Select MAX(table_id) 
                              FROM [shc-pricing-prod:static_tables.__TABLES__]
                              where table_id contains \"static_table_MM\")')) a
left join 
(select * from 
TABLE_QUERY([shc-pricing-prod:min_comp_tables], 
      'table_id CONTAINS \"min_comp\" 
      AND table_id= (Select MAX(table_id) 
                              FROM [shc-pricing-prod:min_comp_tables.__TABLES__]
                              where table_id contains \"min_comp_all\")')) b
on a.div_no = b.div_no and a.itm_no = b.itm_no
left join 
(select * from 
TABLE_QUERY([shc-pricing-prod:min_comp_tables], 
      'table_id CONTAINS \"min_comp_MM\" 
      AND table_id= (Select MAX(table_id) 
                              FROM [shc-pricing-prod:min_comp_tables.__TABLES__]
                              where table_id contains \"min_comp_MM\")')) c
on a.div_no = c.div_no and a.itm_no = c.itm_no
join [shc-pricing-dev:static_lists.Automotive_5pct] d
on a.div_no = d.div_no and a.itm_no = d.itm_no )
order by a.itm_no"
