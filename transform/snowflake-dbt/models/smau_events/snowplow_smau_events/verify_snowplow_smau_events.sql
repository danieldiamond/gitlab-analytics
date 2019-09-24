{{ config({
    "materialized": "incremental",
    "unique_key": "page_view_id"
    })
}}

{%- set event_ctes = ["gitlab_ci_yaml_edited",
                      "gitlab_ci_yaml_viewed",
                      "job_list_viewed",
                      "job_viewed",
                      "pipeline_charts_viewed",
                      "pipeline_list_viewed",
                      "pipeline_schedules_viewed",
                      "pipeline_viewed"
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
  WHERE page_view_start >= '2019-01-01'
  {% if is_incremental() %}
    AND page_view_start >= (SELECT MAX(event_date) FROM {{this}})
  {% endif %}

)

, gitlab_ci_yaml_edited AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'gitlab_ci_yaml_edited'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/.gitlab-ci.yml'
    AND page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/edit\/(.)*'
)

, gitlab_ci_yaml_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'gitlab_ci_yaml_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/.gitlab-ci.yml'
    AND page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/blob\/(.)*'

)

, job_list_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'job_list_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/-\/jobs'

)

, job_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'job_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/-\/jobs\/[0-9]{1,}'

)

, pipeline_charts_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'pipeline_charts_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/pipelines\/charts'

)

, pipeline_list_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'pipeline_list_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/pipelines'

)

, pipeline_schedules_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'pipeline_schedules_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/pipeline_schedules'

)

, pipeline_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'pipeline_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/pipelines\/[a-9]{1,}'

)

SELECT * 
FROM pipeline_viewed
