bands_raw=$(bq query "SELECT nth(51,quantiles(MR_30,101)) MR_median,nth(51,quantiles(conversion_30,101)) conversion_median,nth(34,quantiles(visits_30,101)) visit_33pct,nth(68,quantiles(visits_30,101)) visit_66pct
FROM TABLE_QUERY([shc-pricing-prod:static_tables], 'table_id CONTAINS \"static_table\" AND table_id= (Select MAX(table_id) FROM [shc-pricing-prod:static_tables.__TABLES__] where table_id contains \"static_table\")') ")
oldIFS="$IFS"
IFS="|"
bands=( $bands_raw )
IFS="$oldIFS"
MR_median=${bands[6]}
conver_median=${bands[7]}
visit_33pct=${bands[8]}
visit_67pct=${bands[9]}

current_date=$(date +%Y-%m-%d)
day_partition=$(date +%Y%m%d)
HOUR=$(date '+%H')
hour_partition=$day_partition$HOUR
bq_partitioned_table="shc-pricing-prod:rule_tables.Div9"'_'"${hour_partition}"
bq query --destination_table=$bq_partitioned_table "
select *, round((Recom_prc - a.cost_with_subsidy)/Recom_prc,2) pushed_MR from 
(select a.*, b.min_comp, b.min_comp_NM, c.min_comp_MM, c.min_comp_MM_NM, 	
(case when c.min_comp_MM is null
then (case when a.blocked is null and a.PMI is not null and a.cost_with_subsidy is not null and (a.ad_plan <> 'Y' or a.ad_plan is null) and a.conversion_30 is not null and a.visits_30 is not null and a.MR_30 is not null
         then (case when a.conversion_30 > ${conver_median} and a.MR_30 > ${MR_median} then (case when a.visits_30 > ${visit_67pct} then  a.PMI 
                                                                                                      when a.visits_30 <= ${visit_67pct} and a.visits_30 > ${visit_33pct} then a.PMI * 1.01
                                                                                                      else a.PMI * 1.02
                                                                                                 end)
                    when a.conversion_30 <= ${conver_median} and a.MR_30 > ${MR_median} then (case when a.visits_30 > ${visit_67pct} then  a.PMI * 0.95
                                                                                                      when a.visits_30 <= ${visit_67pct} and a.visits_30 > ${visit_33pct} then a.PMI * 0.97
                                                                                                      else a.PMI * 0.98
                                                                                                 end)
                    when a.conversion_30 > ${conver_median} and a.MR_30 <= ${MR_median} then (case when a.visits_30 > ${visit_67pct} then  a.PMI * 1.01
                                                                                                      when a.visits_30 <= ${visit_67pct} and a.visits_30 > ${visit_33pct} then a.PMI * 1.03
                                                                                                      else a.PMI * 1.05
                                                                                                  end)
            else (case when a.visits_30 > ${visit_67pct} then  a.PMI * 1.03
                           when a.visits_30 <= ${visit_67pct} and a.visits_30 > ${visit_33pct} then a.PMI * 1.05
                           else a.PMI * 1.05
                      end)
                        end)
          end)
else (
 case when a.blocked is NULL AND (a.ad_plan <> 'Y' or a.ad_plan <> 'Y' is NULL) AND a.cost_with_subsidy is not NULL
 then (case when d.pi_bin is NULL OR a.MR_30 is NULL
       then c.min_comp_MM
       else (case when(a.ASP_30 <100 and a.conversion_30 >0.075) OR (a.ASP_30 >=100 and a.ASP_30 <250 and a.conversion_30 >0.04) OR (a.ASP_30 >250 and a.conversion_30 >0.025)
             then (case when d.pi_bin='H'
                   then (case when a.MR_30 <=0.25
                         then c.min_comp_MM * 1.04
                         else c.min_comp_MM
                         end)
                   else (case when a.MR_30 <=0.25
                         then c.min_comp_MM * 1.02
                         else c.min_comp_MM
                         end)
                   end)
             else (case when d.pi_bin='H'
                   then (case when a.MR_30 <=0.25
                         then c.min_comp_MM * 1.02
                         else c.min_comp_MM * 0.97
                         end)
                   else (case when a.MR_30 >0.25
                         then c.min_comp_MM * 0.97
                         end)
                   end)
             end)
        end)
 end
)
end)  Recom_prc,
(case when c.min_comp_MM is null
then (case when a.blocked is null and a.PMI is not null and a.cost_with_subsidy is not null and (a.ad_plan <> 'Y' or a.ad_plan is null) and a.conversion_30 is not null and a.visits_30 is not null and a.MR_30 is not null
	 then (case when a.conversion_30 > ${conver_median} and a.MR_30 > ${MR_median} then (case when a.visits_30 > ${visit_67pct} then  'Tools Online Head Visits High Conversion High Margin'
	                                                                                              when a.visits_30 <= ${visit_67pct} and a.visits_30 > ${visit_33pct} then 'Tools Online Core Visits High Conversion High Margin'
	                                                                                              else 'Tools Online Tail Visits High Conversion High Margin'
	                                                                                         end)
		        when a.conversion_30 <= ${conver_median} and a.MR_30 > ${MR_median} then (case when a.visits_30 > ${visit_67pct} then  'Tools Online Head Visits Low Conversion High Margin'
	                                                                                              when a.visits_30 <= ${visit_67pct} and a.visits_30 > ${visit_33pct} then 'Tools Online Core Visits Low Conversion High Margin'
	                                                                                              else 'Tools Online Tail Visits Low Conversion High Margin'
	                                                                                         end)
				when a.conversion_30 > ${conver_median} and a.MR_30 <= ${MR_median} then (case when a.visits_30 > ${visit_67pct} then  'Tools Online Head Visits High Conversion Low Margin'
	                                                                                              when a.visits_30 <= ${visit_67pct} and a.visits_30 > ${visit_33pct} then 'Tools Online Core Visits High Conversion Low Margin'
	                                                                                              else 'Tools Online Tail Visits Low Conversion High Margin'
	                                                                                         end)
            else (case when a.visits_30 > ${visit_67pct} then  'Tools Online Head Visits Low Conversion Low Margin'
	                   when a.visits_30 <= ${visit_67pct} and a.visits_30 > ${visit_33pct} then 'Tools Online Core Visits Low Conversion Low Margin'
	                   else 'Tools Online Tail Visits Low Conversion Low Margin'
	              end)
			end)
	  end)
else (
 case when a.blocked is NULL AND (a.ad_plan <> 'Y' or a.ad_plan <> 'Y' is NULL) AND a.cost_with_subsidy is not NULL 
 then (case when d.pi_bin is NULL OR a.MR_30 is NULL 
       then 'Tools New Matched No ASP or No PI'
       else (case when(a.ASP_30 <100 and a.conversion_30 >0.075) OR (a.ASP_30 >=100 and a.ASP_30 <250 and a.conversion_30 >0.04) OR (a.ASP_30 >250 and a.conversion_30 >0.025)
             then (case when d.pi_bin='H' 
                   then (case when a.MR_30 <=0.25 
                         then 'High PI Low Margin High CV'
                         else 'High PI High Margin High CV'
                         end) 
                   else (case when a.MR_30 <=0.25 
                         then  'Low PI Low Margin High CV'
                         else 'Low PI High Margin High CV' 
                         end) 
                   end)
             else (case when d.pi_bin='H' 
                   then (case when a.MR_30 <=0.25 
                         then 'High PI Low Margin Low CV' 
                         else 'High PI High Margin Low CV'
                         end)
                   else (case when a.MR_30 >0.25 
                         then 'Low PI High Margin Low CV'
                         end) 
                   end) 
             end) 
        end)
 end)	  
end) Rule_name,
'1-Division' Rule_Level
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
join [shc-pricing-dev:static_lists.pi_bin_static_list] d
on a.div_no = d.div_no and a.itm_no = d.itm_no 
where a.div_no = 9)
order by a.itm_no"
