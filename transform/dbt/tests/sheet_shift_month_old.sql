with base as (
	
  SELECT max(to_timestamp(updated_at)::date), 'sales_quota' FROM historical.sales_quota
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'headcount' FROM historical.headcount
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'metrics' FROM historical.metrics
  UNION ALL
  SELECT max(to_timestamp(updated_at)::date), 'regional_quotas' FROM historical.transposed


),  maxdate as (

    select 	*,
               CURRENT_DATE - max as last_update_diff
    from base

)

select *
from maxdate 
WHERE last_update_diff >= '36' 
