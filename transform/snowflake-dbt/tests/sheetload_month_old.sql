{% set find_max_date = "max(DATEADD(S, _updated_at, '1970-01-01')::date) as max_date" %}

{% set table_names = [('headcount', 'raw.sheetload.headcount'),
                      ('metrics', 'raw.sheetload.metrics'),
                      ('sales_quotas_region_by_month', 'raw.sheetload.sales_quotas_region_by_month'),
                      ('sales_quotas_region_by_fq', 'raw.sheetload.sales_quotas_region_by_fq'),
                      ('sales_ramp_time_by_month', 'raw.sheetload.sales_ramp_time_by_month'),
                      ('sales_ramp_time_by_tenure', 'raw.sheetload.sales_ramp_time_by_tenure'),
                      ('sales_quotas', 'raw.sheetload.sales_quotas'),
                      ('sales_rep_productivity_all', 'raw.sheetload.sales_rep_productivity_all'),
                      ('sales_rep_productivity_sal', 'raw.sheetload.sales_rep_productivity_sal'),
                      ('sales_rep_productivity_mm_ae', 'raw.sheetload.sales_rep_productivity_mm_ae'),
                      ('sales_rep_productivity_smb_ae', 'raw.sheetload.sales_rep_productivity_smb_ae')
                      ] %}

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
