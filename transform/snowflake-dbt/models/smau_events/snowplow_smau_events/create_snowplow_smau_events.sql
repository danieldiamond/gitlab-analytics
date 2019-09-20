{{ config({
    "materialized": "incremental",
    "unique_key": "page_view_id"
    })
}}

{%- set event_ctes = ["mr_viewed",
                      "project_viewed_in_ide",
                      "repo_file_viewed",
                      "search_performed",
                      "snippet_created",
                      "snippet_edited",
                      "snippet_viewed",
                      "wiki_page_viewed"
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

, mr_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'mr_viewed'              AS event_type,
    page_view_id


  FROM snowplow_page_views
  WHERE page_url_path RLIKE '(\/([0-9A-Za-z_.-])*){2}\/merge_requests/[0-9]*'
    AND page_url_path NOT REGEXP '/-/ide/(.)*'
    -- removing wiki pages
    AND page_url_path NOT REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/wikis(\/(([0-9A-Za-z_.-]|\%))*){1,2}'

)

, project_viewed_in_ide AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'project_viewed_in_ide'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path RLIKE '/-/ide/project/.*'
  -- removing wiki pages
    AND page_url_path NOT REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/wikis(\/(([0-9A-Za-z_.-]|\%))*){1,2}'


)

, repo_file_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'repo_file_viewed'       AS event_type,
    page_view_id


  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/tree\/(.)*'
    AND page_url_path NOT REGEXP '/-/ide/(.)*'
    AND page_url_path NOT REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/wiki\/tree\/(.)*'
    AND page_url_path NOT REGEXP '((\/([0-9A-Za-z_.-])*){2,})?\/snippets/[0-9]{1,}'
    -- removing wiki pages
    AND page_url_path NOT REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/wikis(\/(([0-9A-Za-z_.-]|\%))*){1,2}'

)

, search_performed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'search_performed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path RLIKE '/search'
  -- removing wiki pages
    AND page_url_path NOT REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/wikis(\/(([0-9A-Za-z_.-]|\%))*){1,2}'

)

, snippet_created AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'snippet_created'        AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path RLIKE '((\/([0-9A-Za-z_.-])*){2,})?\/snippets/new'
)

, snippet_edited AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'snippet_edited'         AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path RLIKE '((\/([0-9A-Za-z_.-])*){2,})?\/snippets/[0-9]*/edit'
)

, snippet_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'snippets_viewed'        AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path RLIKE '((\/([0-9A-Za-z_.-])*){2,})?\/snippets/[0-9]{1,}'
    -- removing wiki pages
    AND page_url_path NOT REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/wikis(\/(([0-9A-Za-z_.-]|\%))*){1,2}'

)

, wiki_page_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'wiki_page_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path RLIKE '(\/([0-9A-Za-z_.-])*){2,}\/wikis(\/(([0-9A-Za-z_.-]|\%))*){1,2}'
    AND page_url_path NOT REGEXP '/-/ide/(.)*'
    -- removing wiki pages
    AND page_url_path NOT REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/wikis(\/(([0-9A-Za-z_.-]|\%))*){1,2}'

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
