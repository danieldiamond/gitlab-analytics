{% set find_max_date = "max(DATEADD(S, updated_at, '1970-01-01')::date) as max_date" %}

{% set table_names = [('sales_quota', 'raw.sheetload.sales_quota'),
                      ('headcount', 'raw.sheetload.headcount'),
                      ('metrics', 'raw.sheetload.metrics'),
                      ('regional_quotas', 'raw.sheetload.transposed'),
                      ('sales_weekly_forecast', 'raw.sheetload.sales_weekly_forecast'),
                      ('ccodashboard_actuals', 'raw.sheetload.ccodashboard_actuals'),
                      ('crodashboard_actuals', 'raw.sheetload.crodashboard_actuals'),
                      ('cfodashboard_actuals', 'raw.sheetload.cfodashboard_actuals'),
                      ('vpedashboard_actuals', 'raw.sheetload.vpedashboard_actuals'),
                      ('cmodashboard_actuals', 'raw.sheetload.cmodashboard_actuals'),
                      ('alliancesdashboard_actuals', 'raw.sheetload.alliancesdashboard_actuals')]%}

with base as (
{% for table_name in table_names -%}
  SELECT {{ find_max_date }}, '{{table_name[0]}}' FROM {{table_name[1]}}
  {% if not loop.last %} 
  UNION ALL {% endif %}
{% endfor -%}

),  maxdate as (

    select 	*,
            datediff(day, max_date, CURRENT_DATE) as last_update_diff
    from base

)

select *
from maxdate 
WHERE last_update_diff >= '36' 
