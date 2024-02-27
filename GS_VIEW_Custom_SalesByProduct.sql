(SELECT *,
revenue_amount  as current_year_revenue_amount,
0 as previous_year_revenue_amount,
cogs_amount as current_year_cogs_amount,
0 as previous_year_cogs_amount
FROM `{{project | sqlsafe}}.{{dataset | sqlsafe}}.Report_SalesAnalysisTransaction` where trandate between PARSE_DATE('%Y%m%d', @DS_START_DATE) and PARSE_DATE('%Y%m%d', @DS_END_DATE)
) union all 
(
SELECT *,
0 as current_year_revenue_amount,
revenue_amount  as previous_year_revenue_amount,
0 as current_year_cogs_amount,
cogs_amount as previous_year_cogs_amount
FROM `{{project | sqlsafe}}.{{dataset | sqlsafe}}.Report_SalesAnalysisTransaction` where trandate between DATE_SUB(PARSE_DATE('%Y%m%d', @DS_START_DATE), INTERVAL 1 YEAR) and DATE_SUB(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 1 YEAR)
  
)