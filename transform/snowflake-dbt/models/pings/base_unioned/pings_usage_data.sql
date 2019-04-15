{% set names = ['pings_usage_data_version', 'pings_usage_data_tappg'] %}

with {% for name in names %} {{name}} as (
    SELECT *
    FROM {{ref(name)}}
    WHERE hostname != 'staging.gitlab.com'
), {%- endfor -%} unioned as (

{%- for name in names -%} 
    SELECT *
    FROM {{name}}
    WHERE hostname != 'staging.gitlab.com'
    {% if not loop.last %} UNION ALL {% endif %}
{%- endfor -%}

)

SELECT * 
FROM unioned
{{ dbt_utils.group_by(n=22) }}
{% if target.name == "ci" %}
where created_at > dateadd(day, -8, current_date)
{% endif  %}