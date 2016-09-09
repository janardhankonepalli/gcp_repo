current_date=$(date +%Y-%m-%d)
day_partition=$(date +%Y%m%d)
HOUR=$(date '+%H')
hour_partition=$day_partition$HOUR
bq query --maximum_billing_tier 2 --destination_table=shc-pricing-prod:all_comp_tables.all_comp_all_no95_1 "
SELECT
  CONCAT(a.website_name,'-', b.sih_sears_division_nbr,'-', b.sih_sears_item_nbr) concat,
  website_name,
  FLOAT(LTRIM(RTRIM(shipping))) shipping,
  FLOAT(LTRIM(RTRIM(purchase_price))) price,
  INTEGER(LTRIM(RTRIM(b.sih_sears_division_nbr))) div_no,
  INTEGER(LTRIM(RTRIM(b.sih_sears_item_nbr))) itm_no,
  last_crawled_date
FROM
  [shc-pricing-prod:bq_pricing_it.smith__marketrack_output_history] a
JOIN
  [shc-pricing-prod:bq_pricing_it.smith_pricing_hub_market_track_map] b
ON
  a.user_label = b.usr_lbl
WHERE
  a._PARTITIONTIME = TIMESTAMP(DATE_ADD('${current_date}',-2,'DAY'))
  AND DATE(last_crawled_date) IN (DATE(DATE_ADD('${current_date}',-2,'DAY')),
    DATE(DATE_ADD('${current_date}',-1,'DAY')),
    '${current_date}')
  AND stock_status != 'OOS'
GROUP BY 1,2,3,4,5,6,7
  HAVING  div_no not in (95,28)"
bq query --maximum_billing_tier 2 --destination_table=shc-pricing-prod:all_comp_tables.all_comp_all_no95_2 "
SELECT
  CONCAT(a.website_name,'-', b.sih_sears_division_nbr,'-', b.sih_sears_item_nbr) concat,
  website_name,
  FLOAT(LTRIM(RTRIM(shipping))) shipping,
  FLOAT(LTRIM(RTRIM(purchase_price))) price,
  INTEGER(LTRIM(RTRIM(b.sih_sears_division_nbr))) div_no,
  INTEGER(LTRIM(RTRIM(b.sih_sears_item_nbr))) itm_no,
  last_crawled_date
FROM
  [shc-pricing-prod:bq_pricing_it.smith__marketrack_output_history] a
JOIN
  [shc-pricing-prod:bq_pricing_it.smith_pricing_hub_market_track_map] b
ON
  a.user_label = b.usr_lbl
WHERE
  a._PARTITIONTIME = TIMESTAMP(DATE_ADD('${current_date}',-1,'DAY'))
  AND DATE(last_crawled_date) IN (DATE(DATE_ADD('${current_date}',-2,'DAY')),
    DATE(DATE_ADD('${current_date}',-1,'DAY')),
    '${current_date}')
  AND stock_status != 'OOS'
GROUP BY 1,2,3,4,5,6,7
  HAVING  div_no not in (95,28)"
bq query --maximum_billing_tier 2 --destination_table=shc-pricing-prod:all_comp_tables.all_comp_all_no95_3 "
SELECT
  CONCAT(a.website_name,'-', b.sih_sears_division_nbr,'-', b.sih_sears_item_nbr) concat,
  website_name,
  FLOAT(LTRIM(RTRIM(shipping))) shipping,
  FLOAT(LTRIM(RTRIM(purchase_price))) price,
  INTEGER(LTRIM(RTRIM(b.sih_sears_division_nbr))) div_no,
  INTEGER(LTRIM(RTRIM(b.sih_sears_item_nbr))) itm_no,
  last_crawled_date
FROM
  [shc-pricing-prod:bq_pricing_it.smith__marketrack_output_history] a
JOIN
  [shc-pricing-prod:bq_pricing_it.smith_pricing_hub_market_track_map] b
ON
  a.user_label = b.usr_lbl
WHERE
  a._PARTITIONTIME =  TIMESTAMP('${current_date}')
  AND DATE(last_crawled_date) IN (DATE(DATE_ADD('${current_date}',-2,'DAY')),
    DATE(DATE_ADD('${current_date}',-1,'DAY')),
    '${current_date}')
  AND stock_status != 'OOS'
GROUP BY 1,2,3,4,5,6,7
  HAVING  div_no not in (95,28)"
bq query --destination_table=shc-pricing-prod:all_comp_tables.all_comp_all_95 "
SELECT 
concat(comp_site_source , '-', srs_div_no ,'-',srs_itm_no) concat,
comp_site_source website_name, 
FLOAT(LTRIM(RTRIM(comp_shipping_cost))) shipping, 
FLOAT(LTRIM(RTRIM(coalesce(comp_price,comp_mean_price )))) price,
INTEGER(LTRIM(RTRIM(srs_div_no))) div_no,
INTEGER(LTRIM(RTRIM(srs_itm_no))) itm_no,
last_updated_dt last_crawled_date
FROM [shc-pricing-prod:bq_pricing_it.smith_ql2_daily_feed]
where _PARTITIONTIME between TIMESTAMP(DATE_ADD('${current_date}',-DAYOFWEEK('${current_date}')+1,'DAY')) and TIMESTAMP('${current_date}')
group by 1,2,3,4,5,6,7
having div_no in (95,28) "
bq_partitioned_table="shc-pricing-prod:all_comp_tables.all_comp_all"'_'"${hour_partition}"
bq query --destination_table=$bq_partitioned_table "
select concat, website_name, coalesce(comp_name, website_name) comp_name, shipping, price, div_no, itm_no, last_crawled_date from (SELECT * FROM 
[shc-pricing-prod:all_comp_tables.all_comp_all_no95_1],
[shc-pricing-prod:all_comp_tables.all_comp_all_no95_2],
[shc-pricing-prod:all_comp_tables.all_comp_all_no95_3],
[shc-pricing-prod:all_comp_tables.all_comp_all_95]
group by 1,2,3,4,5,6,7) a 
left join [shc-pricing-dev:static_lists.competitor_index_mapping] b
on a.website_name = b.comp_index"
bq rm -f -t shc-pricing-prod:all_comp_tables.all_comp_all_no95_1
bq rm -f -t shc-pricing-prod:all_comp_tables.all_comp_all_no95_2
bq rm -f -t shc-pricing-prod:all_comp_tables.all_comp_all_no95_3
bq rm -f -t shc-pricing-prod:all_comp_tables.all_comp_all_95

