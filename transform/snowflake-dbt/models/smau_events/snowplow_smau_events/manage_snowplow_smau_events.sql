{{ config({
    "materialized": "incremental",
    "unique_key": "page_view_id"
    })
}}

{%- set event_ctes = ["audit_events_viewed",
                      "cycle_analytics_viewed",
                      "insights_viewed",
                      "group_analytics_viewed",
                      "group_created",
                      "user_authenticated"
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
  FROM {{ ref('snowplow_page_views')}}
  WHERE TRUE
    AND app_id = 'gitlab'
    AND page_view_start > '2019-08-01' --TODO
  {% if is_incremental() %}
    AND page_view_start >= (SELECT MAX(event_date) FROM {{this}})
  {% endif %}
)

, audit_events_viewed AS (
  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)    AS event_date,
    page_url_path,
    'audit_events_viewed'       AS event_type,
    page_view_id
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/audit_events'
)

, cycle_analytics_viewed AS (
  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'cycle_analytics_viewed'   AS event_type,
    page_view_id
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/cycle_analytics' 
)

, insights_viewed AS (
  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'insights_viewed'          AS event_type,
    page_view_id
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '\/groups(\/([a-zA-Z-])*){1,}\/-\/insights'
)

, group_analytics_viewed AS (
  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'group_analytics_viewed'   AS event_type,
    page_view_id
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '\/groups(\/([a-zA-Z-])*){1,}\/-\/analytics'
)

-- TODO: call "group created" or "new group page viewed"?
, group_created AS (
  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'group_created'            AS event_type,
    page_view_id
  FROM snowplow_page_views
  WHERE page_url_path LIKE '%/groups/new%' --TODO
)

  -- Looks at referrer_url in addition to page_url
, user_authenticated AS (
  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'user_authenticated'       AS event_type,
    page_view_id
  FROM snowplow_page_views
  WHERE referer_url_path LIKE '%users/sign_in%' --TODO
    AND page_url_path NOT REGEXP '/users/*'
)

, unioned AS (
  {% for event_cte in event_ctes %}

    (
      SELECT
        *
      FROM {{ event_cte }}
    )

    {%- if not loop.last -%}
        UNION
    {%- endif %}

  {% endfor -%}

)

SELECT *
FROM unioned
