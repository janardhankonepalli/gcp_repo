#!/bin/bash
day_partition=$(date +%Y%m%d)
all_divs='Div6 Div8 Div95 Div71 Div28 Div34 Div9 Div52'
div_arr=($all_divs)
for div in "${div_arr[@]}"
do
tbl_id_raw=$(bq query "Select MAX(table_id) FROM [shc-pricing-prod.FTP_tables.__TABLES__] where table_id contains \"$div\" and table_id contains \"FTP\" ")
tbl_id=(${tbl_id_raw//|/ })
file=gs://shc-pricing-prod-pricing-it/output_csv_files/"${day_partition}"/${tbl_id[-2]}.csv
if [ -f "$file" ]
then
gsutil rm $file
fi		
bq extract --destination_format=CSV  shc-pricing-prod:FTP_tables.${tbl_id[-2]} $file
done

