{{ config({
    "materialized": "incremental",
    "unique_key": "event_surrogate_key"
    })
}}

{%- set event_ctes = [
   {
      "event_name":"envrionments_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/environments$",
            "regexp_function":"REGEXP"
         },
         {
            "regexp_pattern":"/help/ci/environments",
            "regexp_function":"NOT REGEXP"
         }
      ]
   },
   {
      "event_name":"error_tracking_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/error_tracking",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"logging_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/environments\/[0-9]*\/logs",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"metrics_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/metrics",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"operations_settings_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/settings\/operations",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"prometheus_edited",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/services\/prometheus\/edit",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"tracing_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/tracing",
            "regexp_function":"REGEXP"
         }
      ]
   }
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
