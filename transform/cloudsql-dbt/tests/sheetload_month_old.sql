with base as (
	
  SELECT max(to_timestamp(updated_at)::date), 'sales_quota' FROM historical.sales_quota
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'headcount' FROM historical.headcount
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'metrics' FROM historical.metrics 
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'regional_quotas' FROM historical.transposed
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'sales_weekly_forecast' FROM historical.sales_weekly_forecast
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'ccodashboard_actuals' FROM historical.ccodashboard_actuals
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'crodashboard_actuals' FROM historical.crodashboard_actuals
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'cfodashboard_actuals' FROM historical.cfodashboard_actuals
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'vpedashboard_actuals' FROM historical.vpedashboard_actuals
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'cmodashboard_actuals' FROM historical.cmodashboard_actuals
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'alliancesdashboard_actuals' FROM historical.alliancesdashboard_actuals
  
),  maxdate as (

    select 	*,
               CURRENT_DATE - max as last_update_diff
    from base

)

select *
from maxdate 
WHERE last_update_diff >= '36' 
