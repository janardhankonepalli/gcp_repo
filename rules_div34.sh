current_date=$(date +%Y-%m-%d)
day_partition=$(date +%Y%m%d)
HOUR=$(date '+%H')
hour_partition=$day_partition$HOUR
bq_partitioned_table="shc-pricing-prod:rule_tables.Div34"'_'"${hour_partition}"
bq query --destination_table=$bq_partitioned_table "
select *, round((Recom_prc - a.cost_with_subsidy)/Recom_prc,2) pushed_MR from 
(select a.*, b.min_comp, b.min_comp_NM, c.min_comp_MM, c.min_comp_MM_NM, 
(case when c.min_comp_MM is not null 
then (case when a.blocked is null and (a.ad_plan <> 'Y' or a.ad_plan is null) and a.cost_with_subsidy is not null
	  then (case when e.tools_min_price is not null 
		    then c.min_comp_MM * (1 + e.tools_min_price) end)
	 end)
else (case when a.blocked is null and (a.ad_plan <> 'Y' or a.ad_plan is null) and a.cost_with_subsidy is not null and (a.PMI is not null or a.reg is not null)
      then (case when a.PMI is not null and e.tools_pmi_price is not null then a.PMI * (1 + e.tools_pmi_price)
	             when a.PMI is null and a.reg is not null and e.tools_pmi_price is not null then a.reg * (1 + e.tools_pmi_price)
	        end)      
	  end)
end) Recom_prc,
(case when c.min_comp_MM is not null 
then (case when a.blocked is null and (a.ad_plan <> 'Y' or a.ad_plan is null) and a.cost_with_subsidy is not null
	  then (case when e.tools_min_price is not null 
		    then 'Tools Online Matched Core Rule' end)
	 end)
else (case when a.blocked is null and (a.ad_plan <> 'Y' or a.ad_plan is null) and a.cost_with_subsidy is not null and (a.PMI is not null or a.reg is not null)
      then (case when a.PMI is not null and e.tools_pmi_price is not null then 'Tools Online Unmatched Core PMI Not Null'
	             when a.PMI is null and a.reg is not null and e.tools_pmi_price is not null then 'Tools Online Unmatched Core Reg Price Not Null'
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
join [shc-pricing-dev:static_lists.Electrical_Whitelist] d
on a.div_no = d.div_no and a.itm_no = d.itm_no 
join [shc-pricing-dev:static_lists.electrical_multipliers] e
on a.div_no = e.div_no and a.itm_no = e.itm_no )
order by a.itm_no"
