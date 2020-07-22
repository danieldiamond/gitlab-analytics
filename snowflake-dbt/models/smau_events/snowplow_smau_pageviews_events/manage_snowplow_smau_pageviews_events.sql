{{ config({
    "materialized": "incremental",
    "unique_key": "event_surrogate_key"
    })
}}

{%- set event_ctes = [
   {
      "event_name":"audit_events_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){1,}\/audit_events",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"cycle_analytics_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/cycle_analytics",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"insights_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){1,}\/insights",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"group_analytics_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){1,}\/analytics",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"group_created",
      "regexp_where_statements":[
         {
            "regexp_pattern":"\/groups\/new",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"productivity_analytics_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){1,}\/productivity_analytics",
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
    page_view_id,
    referer_url_path
  FROM {{ ref('snowplow_page_views_all') }}
  WHERE TRUE
    AND app_id = 'gitlab'
  {% if is_incremental() %}
    AND page_view_start >= (SELECT MAX(event_date) FROM {{this}})
  {% endif %}

)

{% for event_cte in event_ctes %}

, {{ smau_events_ctes(event_name=event_cte.event_name, regexp_where_statements=event_cte.regexp_where_statements) }}

{% endfor -%}

  /*
    Looks at referrer_url in addition to page_url.
    Regex matches for successful sign-in authentications,
    meaning /sign_in redirects to a real GitLab page.
  */
, user_authenticated AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'user_authenticated'       AS event_type,
    {{ dbt_utils.surrogate_key('page_view_id', 'event_type') }}
                               AS event_surrogate_key
  FROM snowplow_page_views
  WHERE referer_url_path REGEXP '\/users\/sign_in'
    AND page_url_path NOT REGEXP '\/users\/sign_in'

)

, unioned AS (

  {% for event_cte in event_ctes %}

    SELECT *
    FROM {{ event_cte.event_name }}

    {%- if not loop.last %}
      UNION
    {%- endif %}

  {% endfor -%}
  
  UNION
  
  SELECT * 
  FROM user_authenticated

)

SELECT *
FROM unioned
