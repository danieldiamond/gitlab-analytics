{{ config({
    "materialized": "incremental",
    "unique_key": "page_view_id"
    })
}}

{%- set event_ctes = ["environments_viewed",
                      "error_tracking_viewed",
                      "logging_viewed",
                      "metrics_viewed",
                      "operations_settings_viewed",
                      "prometheus_edited",
                      "tracing_viewed"
                      ]
-%}

WITH snowplow_page_views AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    page_view_start,
    page_url_path,
    page_view_id
  FROM {{ ref('snowplow_page_views')}}
  WHERE TRUE
  {% if is_incremental() %}
    AND page_view_start >= (SELECT MAX(event_date) FROM {{this}})
  {% endif %}

)

, environments_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'envrionments_viewed'      AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '((\/([0-9A-Za-z_.-])*){2,})?\/environments'
    AND page_url_path NOT IN ('/help/ci/environments')

)

, error_tracking_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)      AS event_date,
    page_url_path,
    'error_tracking_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '((\/([0-9A-Za-z_.-])*){2,})?\/error_tracking'

)

, logging_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'logging_viewed'           AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '((\/([0-9A-Za-z_.-])*){2,})?\/environments\/[0-9]*\/logs'

)

, metrics_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'metrics_viewed'           AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '((\/([0-9A-Za-z_.-])*){2,})?\/metrics'

)

, operations_settings_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'operations_settings_viewed'           AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '((\/([0-9A-Za-z_.-])*){2,})?\/settings\/operations'

)

, prometheus_edited AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'prometheus_edited'           AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '((\/([0-9A-Za-z_.-])*){2,})?\/metrics'

)

, tracing_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'tracing_viewed'           AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '((\/([0-9A-Za-z_.-])*){2,})?\/tracing'

)

, unioned AS (
  {% for event_cte in event_ctes %}

      SELECT
        *
      FROM {{ event_cte }}

    {%- if not loop.last %}
        UNION
    {%- endif %}

  {% endfor -%}

)

SELECT *
FROM unioned
