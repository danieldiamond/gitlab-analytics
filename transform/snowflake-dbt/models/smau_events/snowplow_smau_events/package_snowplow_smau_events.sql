{{ config({
    "materialized": "incremental",
    "unique_key": "page_view_id"
    })
}}

{%- set event_ctes = ["container_registry_viewed",
                      "dependency_proxy_page_viewed",
                      "packages_page_viewed"
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

, container_registry_viewed AS (

  SELECT DISTINCT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'container_registry_viewed'      AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '((\/([0-9A-Za-z_.-])*){,})?\/container_registry$'

)

, dependency_proxy_page_viewed AS (

  SELECT DISTINCT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'dependency_proxy_page_viewed'      AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '((\/([0-9A-Za-z_.-])*){2,})?\/dependency_proxy$'

)

, packages_page_viewed AS (

  SELECT DISTINCT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'packages_page_viewed'      AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '((\/([0-9A-Za-z_.-])*){2,})?\/packages$'

)

, unioned AS (
  {% for event_cte in event_ctes %}

    SELECT *
    FROM {{ event_cte }}

    {%- if not loop.last %}
      UNION
    {%- endif %}

  {% endfor -%}

)

SELECT *
FROM unioned
