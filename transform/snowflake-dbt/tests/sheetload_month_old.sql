with base as (
	
  SELECT max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date, 'sales_quota' FROM raw.historical.sales_quota
  UNION ALL
  SELECT max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date, 'headcount' FROM raw.historical.headcount
  UNION ALL
  SELECT max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date, 'metrics' FROM raw.historical.metrics 
  UNION ALL
  SELECT max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date, 'regional_quotas' FROM raw.historical.transposed
  UNION ALL
  SELECT max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date, 'sales_weekly_forecast' FROM raw.historical.sales_weekly_forecast
  UNION ALL
  --SELECT max(to_timestamp(updated_at)::date), 'ccodashboard_actuals' FROM historical.ccodashboard_actuals
  --UNION ALL
  SELECT max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date, 'crodashboard_actuals' FROM raw.historical.crodashboard_actuals
  UNION ALL
  SELECT max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date, 'cfodashboard_actuals' FROM raw.historical.cfodashboard_actuals
  UNION ALL
  SELECT max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date, 'vpedashboard_actuals' FROM raw.historical.vpedashboard_actuals
  UNION ALL
  SELECT max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date, 'cmodashboard_actuals' FROM raw.historical.cmodashboard_actuals
  UNION ALL
  SELECT max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date, 'alliancesdashboard_actuals' FROM raw.historical.alliancesdashboard_actuals
  
),  maxdate as (

    select 	*,
               datediff(day, max_date, CURRENT_DATE) as last_update_diff
    from base

)

select *
from maxdate 
WHERE last_update_diff >= '36' 
