{{ config({
    "materialized": "incremental",
    "unique_key": "event_surrogate_key"
    })
}}

{%- set event_ctes = [
    {
      "event_name":"environment_page_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/environments$",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"feature_flags_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/-\/feature_flags$",
            "regexp_function":"REGEXP"
         }
      ]
   },
    {
      "event_name":"release_page_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/-\/releases$",
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
  FROM {{ ref('snowplow_page_views_30')}} --TODO
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
