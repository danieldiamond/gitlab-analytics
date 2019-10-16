{{ config({
    "materialized": "incremental",
    "unique_key": "event_surrogate_key"
    })
}}

{%- set event_ctes = [
   {
      "event_name":"cluster_health_metrics_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/clusters/[0-9]{1,}",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"function_metrics_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/serverless\/functions\/\*\/(.*?)",
            "regexp_function":"REGEXP"
         }
      ]
   },
    {
      "event_name":"serverless_page_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/functions",
            "regexp_function":"REGEXP"
         }
      ]
   },
]
-%}

WITH snowplow_page_views AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    page_view_start,
    page_url_path,
    page_view_id
  FROM {{ ref('snowplow_page_views_all')}}
  WHERE TRUE
  {% if is_incremental() %}
    AND page_view_start >= (SELECT MAX(event_date) FROM {{this}})
  {% endif %}

)

{% for event_cte in event_ctes %}

, {{ smau_events_ctes(event_name=event_cte.event_name, regexp_where_statements=event_cte.regexp_where_statements) }}

{% endfor -%}

, unioned AS (

  {% for event_cte in event_ctes %}

    SELECT *
    FROM {{ event_cte.event_name }}

    {%- if not loop.last %}
      UNION
    {%- endif %}

  {% endfor -%}

)

SELECT *
FROM unioned
