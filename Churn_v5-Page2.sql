select
period_start_date,
category,
sum(value) as value,
if(period_start_date between date_sub(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 3 MONTH) and PARSE_DATE('%Y%m%d', @DS_END_DATE),sum(value),0) as value_3_month,
if(period_start_date between date_sub(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 12 MONTH) and PARSE_DATE('%Y%m%d', @DS_END_DATE),sum(value),0) as value_12_month,
if(period_start_date between date_sub(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 24 MONTH) and date_sub(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 12 month),sum(value),0) as value_13_month,
@DS_END_DATE as End_Date,
@DS_START_DATE as Start_Date,
last_day(period_start_date) as last_day_of_the_month

From

(select
period_start_date
, category
,case when category = '01-Begining' then Beginning_revenue
  when category = '02-Churn' then Churn
  when category = '03-Downsell' then Downsell
  when category = '04-Upsell' then Upsell
  when category = '05-Net Retention in Month' then Net_Retention_In_Month
  when category = '06-New Customer Revenue' then New_Customers
  when category = '07-Ending MRR' then Ending_MRR
  when category = '08-Percentage_Net_Retention_In_Month' then Percentage_Net_Retention_In_Month
  when category = '09-Cloudli Usage Revenue' then Usage_Revenue
  when category = '10-CMV Pass Through Items' then VBS_amount
  when category = '11-Borrowing Base Revenue' then Borrowing_Base_Revenue

end as value

from `{{project | sqlsafe}}.{{dataset | sqlsafe}}._CACHE_Report_Churn_v5_Consolidated_DebugFT` 
  cross join  unnest(array['01-Begining', '02-Churn','03-Downsell','04-Upsell','05-Net Retention in Month', '06-New Customer Revenue', '07-Ending MRR','08-Percentage_Net_Retention_In_Month','09-Cloudli Usage Revenue', '10-CMV Pass Through Items','11-Borrowing Base Revenue']) as category

where period_start_date between date_sub(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 25 month) and PARSE_DATE('%Y%m%d', @DS_END_DATE)


order by period_start_date)

group by period_start_date, category--, value