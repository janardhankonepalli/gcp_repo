current_date=$(date +%Y-%m-%d)
day_partition=$(date +%Y%m%d)
HOUR=$(date '+%H')
hour_partition=$day_partition$HOUR
bq_partitioned_table="shc-pricing-prod:rule_tables.Div52"'_'"${hour_partition}"
bq query --destination_table=$bq_partitioned_table "
select *, round((Recom_prc - a.cost_with_subsidy)/Recom_prc,2) pushed_MR from 
(select a.*, b.min_comp, b.min_comp_NM, c.min_comp_MM, c.min_comp_MM_NM,
(case when b.min_comp is not null   
 then (case when blocked is null and ((a.ad_plan <> 'Y' and a.ad_plan <> 'D') or a.ad_plan is null)
	   then (case when visits_30 is not null 
			   then (case when c.min_comp_MM is not null
					 then (case when  b.min_comp  = c.min_comp_MM  
						   then (case when (c.min_comp_MM - cost)/c.min_comp_MM >= 0.25 and visits_30  >= 10 then c.min_comp_MM  * 0.99   
								 when (c.min_comp_MM - cost)/c.min_comp_MM >= 0.25 and visits_30  < 10 then c.min_comp_MM   
								 when (c.min_comp_MM - cost)/c.min_comp_MM < 0.25  and visits_30  >= 10 then c.min_comp_MM 
								 else c.min_comp_MM * 1.02 end)
						   else ( case when PMI is not null 
											then (case when PMI >= c.min_comp_MM then c.min_comp_MM else PMI end)
								  else c.min_comp_MM  end)
						   end)
					  else  (case when PMI is not null 
									   then (case when PMI >= a.min_margin then a.min_margin else PMI end)
							 else a.min_margin  end)
					  end)
				else (case when c.min_comp_MM is not null
				      then  c.min_comp_MM
					  else (case when PMI is not null 
									   then (case when PMI >= a.min_margin then a.min_margin else PMI end)
							 else a.min_margin  end)
				      end)
				end)
        end)  
else ( case when blocked is null and ( reg is not null or PMI is not null) 
       then (case when PMI is not null  
            then (case when visits_30 is not null 
                then (case when 1- cost/PMI >= 0.25 and visits_30 >= 10 then PMI * 0.99  
                           when 1- cost/PMI >= 0.25 and visits_30 < 10 then PMI  
                           when 1- cost/PMI < 0.25 and visits_30 >= 10 then PMI  
                      else PMI * 1.02 end)  
                 else PMI * 0.9975 end)
            else reg * 0.95 end)
       end)     
end ) Recom_Prc,
(case when b.min_comp is not null   
 then (case when blocked is null and ((a.ad_plan <> 'Y' and a.ad_plan <> 'D') or a.ad_plan is null)
	   then (case when visits_30 is not null 
			   then (case when c.min_comp_MM is not null
					 then (case when  b.min_comp  = c.min_comp_MM  
						   then (case when (c.min_comp_MM - cost)/c.min_comp_MM >= 0.25 and visits_30  >= 10 then 'M_HH'    
								 when (c.min_comp_MM - cost)/c.min_comp_MM >= 0.25 and visits_30  < 10 then 'M_HL'   
								 when (c.min_comp_MM - cost)/c.min_comp_MM < 0.25  and visits_30  >= 10 then 'M_LH' 
								 else 'M_LL' end)
						   else ( case when PMI is not null 
											then (case when PMI >= c.min_comp_MM then 'MM_MC' else 'MM_PMI'  end)
								  else 'MM_MC'  end)
						   end)
					  else  (case when PMI is not null 
									   then (case when PMI >= a.min_margin then 'CA_MM' else 'CA_PMI' end)
							 else 'CA_MM'  end)
					  end)
				else (case when c.min_comp_MM is not null
				      then  'CA_MC'
					  else (case when PMI is not null 
									   then (case when PMI >= a.min_margin then 'CA_MM' else 'CA_PMI' end)
							 else 'CA_MM'  end)
				      end)
				end)
        end)
else ( case when blocked is null and ( reg is not null or PMI is not null) 
       then (case when PMI is not null  
            then (case when visits_30 is not null 
                then (case when 1- cost/PMI >= 0.25 and visits_30 >= 10 then 'UM_HH'
                           when 1- cost/PMI >= 0.25 and visits_30 < 10 then 'UM_HL'  
                           when 1- cost/PMI < 0.25 and visits_30 >= 10 then 'UM_LH'  
                      else 'UM_LL' end)  
                 else 'UM_CA_PMI' end)
            else 'UM_Creg' end)
       end)     
end )  Rule_Name, 
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
where a.div_no = 52 )
order by a.itm_no"

