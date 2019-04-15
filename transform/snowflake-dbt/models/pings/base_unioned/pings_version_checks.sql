{{ config(schema='analytics') }}

{% set names = ['pings_version_checks_version', 'pings_version_checks_tappg'] %}

with {% for name in names %} {{name}} as (
    SELECT *
    FROM {{ref(name)}}
), {%- endfor -%} unioned as (

{%- for name in names -%} 
    SELECT *
    FROM {{name}}
    {% if not loop.last %} UNION ALL {% endif %}
{%- endfor -%}

)

SELECT * 
FROM unioned
{{ dbt_utils.group_by(n=7) }}
{% if target.name == "ci" %}
where created_at > dateadd(day, -8, current_date)
{% endif  %}