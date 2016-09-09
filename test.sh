current_date=$(date +%Y-%m-%d)
day_partition=$(date +%Y%m%d)
bq_partitioned_table="runtime_temp_tables.static__table"'_'"${day_partition}"
bq query --destination_table=$bq_partitioned_table " 
select a.div_no div_no, a.ln_no ln_no, 
b.sub_ln_no sub_ln_no, b.cls_no cls_no, b.BU_no BU_no, b.BU_desc BU_desc, 
a.itm_no itm_no, a.Product_Brand Product_Brand, a.reg reg, c.cost cost, 
d.brand brand, d.isdispelig isdispelig,  
e.override_cost override_cost,  
case when e.override_cost is not null and c.cost is not null then c.cost- e.override_cost 
     when e.override_cost is null and c.cost is not null then c.cost 
	 end cost_with_subsidy, 
f.online_exclusive online_exclusive, o.blocked blocked, o.Blocked_reason Blocked_reason, p.ad_plan ad_plan, g.DP_blocked DP_blocked, h.HUB_blocked HUB_blocked, 
i.map_value MAP_value, case when i.map_value is null or i.map_value = 0 then 'N' else 'Y' end MAP_rule, i.MAP_price MAP_price, j.PMI PMI, 
t.visits_1 visits_1,t.orders_1 orders_1, t.conversion_1 conversion_1,
k.visits_7 visits_7,k.orders_7 orders_7, k.conversion_7 conversion_7, 
l.visits_30 visits_30,l.orders_30 orders_30, l.conversion_30 conversion_30,  
m.visits_90 visits_90,m.orders_90 orders_90, m.conversion_90 conversion_90, 
s.sales_1 sales_1 ,s.units_1 units_1 ,s.margin_1 margin_1, s.ASP_1 ASP_1, s.MR_1 MR_1,
n.sales_7 sales_7 ,n.units_7 units_7 ,n.margin_7 margin_7, n.ASP_7 ASP_7, n.MR_7 MR_7,
q.sales_30 sales_30,q.units_30 units_30,q.margin_30 margin_30, q.ASP_30 ASP_30, q.MR_30 MR_30,
r.sales_90 sales_90,r.units_90 units_90,r.margin_90 margin_90, r.ASP_90 ASP_90, r.MR_90 MR_90,
timestamp('${current_date}') date  
from   
(SELECT integer(rtrim(ltrim(Div))) div_no,  integer(rtrim(ltrim(Line))) ln_no,  integer(rtrim(ltrim(Item))) itm_no, Product_Brand ,min(float(rtrim(ltrim(Reg_Price)))) reg  
FROM [shc-pricing-prod:bq_pricing_it.smith_sears_hierarchy_data]   
where _PARTITIONTIME = timestamp('${current_date}')  
group by 1,2,3,4) a   
left join   
(SELECT max(integer(rtrim(ltrim(sears_business_nbr)))) BU_no, max(sears_business_desc) BU_desc, integer(rtrim(ltrim(sears_division_nbr))) div_no, 
max(integer(rtrim(ltrim(sears_sub_line_nbr)))) sub_ln_no, max(integer(rtrim(ltrim(sears_class_nbr)))) cls_no, integer(rtrim(ltrim(sears_item_nbr))) itm_no  
FROM [shc-pricing-prod:bq_pricing_it.gold__item_sears_hierarchy_current]   
group by 3,6) b  
on a.div_no = b.div_no and a.itm_no = b.itm_no  
left join  
(SELECT integer(ltrim(rtrim(div_no))) div_no, integer(ltrim(rtrim(itm_no))) itm_no, float(rtrim(ltrim(cost))) cost FROM [shc-pricing-prod:bq_pricing_it.smith_sears_cost_data]  
where _PARTITIONTIME = timestamp('${current_date}') group by 1,2,3) c 
on a.div_no = c.div_no and a.itm_no = c.itm_no  
left join  
(SELECT integer(ltrim(rtrim(prcm_sears_div_nbr))) div_no, integer(ltrim(rtrim(prcm_sears_itm_nbr))) itm_no, max(brand) brand, 'True' isdispelig  FROM [shc-pricing-prod:bq_pricing_it.incoming__onlineitems_price_w_hierarchy]  
where _PARTITIONTIME = timestamp('${current_date}')  
and isdispelig = 'true' 
and format_id = 'sears'  
group by 1,2,4) d  
on a.div_no = d.div_no and a.itm_no = d.itm_no 
left join  
(SELECT integer(ltrim(rtrim(div_no))) div_no , integer(ltrim(rtrim(itm_no))) itm_no ,  
float(ltrim(rtrim(override_cost))) override_cost FROM [shc-pricing-prod:bq_pricing_it.smith_sears_subsidy_data]  
where _PARTITIONTIME = timestamp('${current_date}') group by 1,2,3) e  
on a.div_no = e.div_no and a.itm_no = e.itm_no  
left join  
( SELECT integer(ltrim(rtrim(div_no))) div_no, integer(ltrim(rtrim(item_no))) itm_no, 'exclusive' online_exclusive FROM [shc-pricing-prod:bq_pricing_it.smith_sears_exclusive_items] 
where _PARTITIONTIME = timestamp('${current_date}') 
group by 1,2  
) f  
on a.div_no = f.div_no and a.itm_no = f.itm_no 
left join  
(SELECT integer(ltrim(rtrim(Div))) div_no, integer(ltrim(rtrim(Item))) itm_no, 
'blocked' blocked, max(Blocked_Reason)  Blocked_reason 
FROM [shc-pricing-prod:bq_pricing_it.smith_sears_block_items] where _PARTITIONTIME = timestamp('${current_date}') and Format_ID = '1' 
group by 1,2) o 
on a.div_no = o.div_no and a.itm_no = o.itm_no  
left join  
(SELECT  
  INTEGER(LTRIM(RTRIM(DIV_NO))) div_no,  
  INTEGER(LTRIM(RTRIM(ITM_NO))) itm_no,  
  max(FORCE_ITM_FL) ad_plan  
FROM  
  [shc-pricing-prod:bq_pricing_it.smith_sears_ad_data]  
WHERE  
  STA_DT <= '${current_date}'  
  AND STP_DT >= '${current_date}'  
GROUP BY 1,2) p  
on a.div_no = p.div_no and a.itm_no = p.itm_no  
left join  
(SELECT  
  INTEGER(ltrim(rtrim(bb.sears_division_nbr))) div_no, 
  integer(ltrim(rtrim(bb.sears_item_nbr))) itm_no,  
  'blocked' AS DP_blocked  
FROM  
 [shc-pricing-prod:bq_pricing_it.incoming__dp_blocked_items] aa  
JOIN  
  [shc-pricing-prod:bq_pricing_it.gold__item_sears_hierarchy_current] bb  
ON  
  aa.item_id = bb.ksn_id  
WHERE aa._PARTITIONTIME = timestamp(date(date_add('${current_date}',-1,'DAY')))  
and aa.fmt_id = '1'  
GROUP BY div_no, itm_no) g  
on a.div_no = g.div_no and a.itm_no = g.itm_no  
left join  
(SELECT integer(ltrim(rtrim(SRS_DIV_NO))) div_no , integer(ltrim(rtrim(SRS_ITEM_NO))) itm_no, 'blocked' HUB_blocked  
FROM [shc-pricing-prod:bq_pricing_it.smith_hub_online_price] where PROMO_START_DATE <= '${current_date}'
and PROMO_END_DATE >= '${current_date}'
and STR_NBR = '9300' and DP_BLOCK_FLAG = 'Y' group by 1,2) h  
on a.div_no = h.div_no and a.itm_no = h.itm_no  
left join  
(  
SELECT integer(left(ltrim(rtrim(div_itm)),3)) div_no, integer(right(ltrim(rtrim(div_itm)),5)) itm_no, max(integer(ltrim(rtrim(map_value )))) map_value, 
 max(float(ltrim(rtrim(map_threshhold)))) MAP_price  
FROM [shc-pricing-prod:bq_pricing_it.smith_sears_map_rules]   
where _PARTITIONTIME = timestamp('${current_date}')   
and map_value is not null 
and div_itm  is not null  
group by 1,2  
) i  
on a.div_no = i.div_no and a.itm_no = i.itm_no  
left join   
(SELECT integer(ltrim(rtrim(div_nbr))) div_no, integer(ltrim(rtrim(item_nbr))) itm_no , 
float(ltrim(rtrim(prc_amt))) PMI  FROM [shc-pricing-prod:bq_pricing_it.smith_pmi_9300_data]  
where _PARTITIONTIME = timestamp('${current_date}')  
group by 1,2,3 ) j  
on a.div_no = j.div_no and a.itm_no = j.itm_no  
left join  
(  
SELECT integer(ltrim(rtrim(div_no))) div_no, integer(ltrim(rtrim(item_no))) itm_no, sum(integer(ltrim(rtrim(visits)))) visits_1,
 sum(integer(ltrim(rtrim(orders)))) orders_1,  
sum(integer(ltrim(rtrim(orders))))/sum(integer(ltrim(rtrim(visits)))) conversion_1  
FROM [shc-pricing-prod:bq_pricing_it.smith_omniture_data] where location_number  ='9300'   
and _PARTITIONTIME  = timestamp('${current_date}')  
group by 1,2  
) t  
on a.div_no = t.div_no and a.itm_no = t.itm_no 
left join  
(  
SELECT integer(ltrim(rtrim(div_no))) div_no, integer(ltrim(rtrim(item_no))) itm_no, sum(integer(ltrim(rtrim(visits)))) visits_7,
 sum(integer(ltrim(rtrim(orders)))) orders_7,  
sum(integer(ltrim(rtrim(orders))))/sum(integer(ltrim(rtrim(visits)))) conversion_7  
FROM [shc-pricing-prod:bq_pricing_it.smith_omniture_data] where location_number  ='9300'   
and _PARTITIONTIME  between timestamp(date_add('${current_date}',-6,'DAY')) and timestamp('${current_date}')  
group by 1,2  
) k  
on a.div_no = k.div_no and a.itm_no = k.itm_no  
left join  
(  
SELECT integer(ltrim(rtrim(div_no))) div_no, integer(ltrim(rtrim(item_no))) itm_no, sum(integer(ltrim(rtrim(visits)))) visits_30, 
sum(integer(ltrim(rtrim(orders)))) orders_30, 
sum(integer(ltrim(rtrim(orders))))/sum(integer(ltrim(rtrim(visits)))) conversion_30  
FROM [shc-pricing-prod:bq_pricing_it.smith_omniture_data] where location_number  ='9300'  
and _PARTITIONTIME  between timestamp(date_add('${current_date}',-29,'DAY')) and timestamp('${current_date}')  
group by 1,2  
) l  
on a.div_no = l.div_no and a.itm_no = l.itm_no  
left join  
(  
SELECT integer(ltrim(rtrim(div_no))) div_no, integer(ltrim(rtrim(item_no))) itm_no, sum(integer(ltrim(rtrim(visits)))) visits_90, sum(integer(ltrim(rtrim(orders)))) orders_90, 
sum(integer(ltrim(rtrim(orders))))/sum(integer(ltrim(rtrim(visits)))) conversion_90  FROM [shc-pricing-prod:bq_pricing_it.smith_omniture_data] where location_number  ='9300'  
and _PARTITIONTIME  between timestamp(date_add('${current_date}',-89,'DAY')) and timestamp('${current_date}')  
group by 1,2  
) m  
on a.div_no = m.div_no and a.itm_no = m.itm_no  
left join   
(  
SELECT
  INTEGER(LTRIM(RTRIM(div_no))) div_no,
  INTEGER(LTRIM(RTRIM(itm_no))) itm_no,
  SUM(INTEGER(LTRIM(RTRIM(units)))) units_7,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL )))) sales_7,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))) - FLOAT(LTRIM(RTRIM(nat_cst_prc )))*INTEGER(LTRIM(RTRIM(units)))) margin_7,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))))/SUM(INTEGER(LTRIM(RTRIM(units)))) ASP_7,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))) - FLOAT(LTRIM(RTRIM(nat_cst_prc )))*INTEGER(LTRIM(RTRIM(units))))/SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL )))) MR_7
FROM
  [shc-pricing-prod:bq_pricing_it.smith_apt_sears_sales]
where _PARTITIONTIME = timestamp('${current_date}')   
and ringing_facility_id_nbr = '0009300' 
and transaction_dt between  date(date_add('${current_date}', -7, 'DAY'))  and date(date_add('${current_date}', -1, 'DAY'))
group by 1,2 
) n  
on a.div_no = n.div_no and a.itm_no = n.itm_no 
left join 
(
SELECT
  INTEGER(LTRIM(RTRIM(div_no))) div_no,
  INTEGER(LTRIM(RTRIM(itm_no))) itm_no,
  SUM(INTEGER(LTRIM(RTRIM(units)))) units_30,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL )))) sales_30,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))) - FLOAT(LTRIM(RTRIM(nat_cst_prc )))*INTEGER(LTRIM(RTRIM(units)))) margin_30,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))))/SUM(INTEGER(LTRIM(RTRIM(units)))) ASP_30,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))) - FLOAT(LTRIM(RTRIM(nat_cst_prc )))*INTEGER(LTRIM(RTRIM(units))))/SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL )))) MR_30
FROM
(SELECT
div_no, itm_no, units, OTD_SALES_FINAL, nat_cst_prc, transaction_dt, date(_PARTITIONTIME) load_dt
FROM
  [shc-pricing-prod:bq_pricing_it.smith_apt_sears_sales]
where _PARTITIONTIME between  date_add('${current_date}', -20, 'DAY') and timestamp('${current_date}')
and ringing_facility_id_nbr = '0009300' 
having (load_dt <> '${current_date}' and transaction_dt  = date(date_add(load_dt, -10, 'DAY')))
  or load_dt='${current_date}')
group by 1,2 
) q
on a.div_no = q.div_no and a.itm_no = q.itm_no
left join 
(
SELECT
  INTEGER(LTRIM(RTRIM(div_no))) div_no,
  INTEGER(LTRIM(RTRIM(itm_no))) itm_no,
  SUM(INTEGER(LTRIM(RTRIM(units)))) units_90,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL )))) sales_90,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))) - FLOAT(LTRIM(RTRIM(nat_cst_prc )))*INTEGER(LTRIM(RTRIM(units)))) margin_90,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))))/SUM(INTEGER(LTRIM(RTRIM(units)))) ASP_90,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))) - FLOAT(LTRIM(RTRIM(nat_cst_prc )))*INTEGER(LTRIM(RTRIM(units))))/SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL )))) MR_90
FROM
(SELECT
div_no, itm_no, units, OTD_SALES_FINAL, nat_cst_prc, transaction_dt, date(_PARTITIONTIME) load_dt
FROM
  [shc-pricing-prod:bq_pricing_it.smith_apt_sears_sales]
where _PARTITIONTIME between  date_add('${current_date}', -80, 'DAY') and timestamp('${current_date}')
and ringing_facility_id_nbr = '0009300' 
having (load_dt <> '${current_date}' and transaction_dt  = date(date_add(load_dt, -10, 'DAY')))
  or load_dt='${current_date}')
group by 1,2 
) r
on a.div_no = r.div_no and a.itm_no = r.itm_no
left join   
(  
SELECT
  INTEGER(LTRIM(RTRIM(div_no))) div_no,
  INTEGER(LTRIM(RTRIM(itm_no))) itm_no,
  SUM(INTEGER(LTRIM(RTRIM(units)))) units_1,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL )))) sales_1,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))) - FLOAT(LTRIM(RTRIM(nat_cst_prc )))*INTEGER(LTRIM(RTRIM(units)))) margin_1,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))))/SUM(INTEGER(LTRIM(RTRIM(units)))) ASP_1,
  SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL ))) - FLOAT(LTRIM(RTRIM(nat_cst_prc )))*INTEGER(LTRIM(RTRIM(units))))/SUM(FLOAT(LTRIM(RTRIM(OTD_SALES_FINAL )))) MR_1
FROM
  [shc-pricing-prod:bq_pricing_it.smith_apt_sears_sales]
where _PARTITIONTIME = timestamp('${current_date}')   
and ringing_facility_id_nbr = '0009300' 
and transaction_dt = date(date_add('${current_date}', -1, 'DAY'))
group by 1,2 
) s  
on a.div_no = s.div_no and a.itm_no = s.itm_no 
"
bq_partitioned_table="runtime_temp_tables.static_table_MM"'_'"${day_partition}"
bq query --destination_table=$bq_partitioned_table " 
select *,  
case when div_no = 95 then cost_with_subsidy/0.95  
     when div_no = 8 and (ln_no = 1 or ln_no = 21 or ln_no = 41 ) then cost_with_subsidy/0.85 
	 when div_no = 8 and ln_no = 55 then cost_with_subsidy/0.8
     when div_no = 6 or div_no = 52 then cost_with_subsidy/0.85  
     when (div_no = 9 or div_no = 34) then cost_with_subsidy/0.85  
     when div_no = 22 or div_no = 26 or div_no = 46 then   
		(case when regexp_match(Product_Brand , r'KENMORE') or regexp_match(brand, r'Kenmore') then cost_with_subsidy/0.665  
		      when regexp_match(brand , r'Samsung') or regexp_match(Product_Brand , r'SAMSUNG') then cost_with_subsidy/0.615  
			  else cost_with_subsidy/0.7  
	     end)  
	 when (div_no = 22 or div_no = 26 or div_no = 46) and regexp_match(Product_Brand , r'KENMORE') then cost_with_subsidy/0.665  
	 when div_no = 71 and ln_no = 22 then cost_with_subsidy/0.875   
	 when div_no = 71 and ln_no <> 22 then cost_with_subsidy/0.875 
     when div_no =28 then cost_with_subsidy/0.75
     end
min_margin 
from 
TABLE_QUERY(runtime_temp_tables, 
      'table_id CONTAINS \"static__table\" 
      AND table_id= (Select MAX(table_id) 
                              FROM runtime_temp_tables.__TABLES__
                              where table_id contains \"static__table\")')
"
