WITH snowplow_page_views AS(
  SELECT
    user_snowplow_domain_id,
    page_view_start,
    page_url_path
  FROM analytics.snowplow_page_views
  WHERE page_view_start >= '2019-07-01'
)

, repo_file_viewed AS (

  SELECT
    user_snowplow_domain_id,
    TO_DATE(page_view_start),
    page_url_path,
    'repo_file_viewed'
FROM snowplow_page_views
WHERE page_url_path RLIKE '.*/tree/.*'
)

, mr_viewed AS (

  SELECT
    user_snowplow_domain_id,
    TO_DATE(page_view_start),
    page_url_path,
    'mr_viewed'
  FROM snowplow_page_views
  WHERE page_url_path regexp '/merge_requests/[0-9]'

)

, wiki_page_viewed AS (

  SELECT
    user_snowplow_domain_id,
    TO_DATE(page_view_start),
    page_url_path,
    'wiki_page_viewed'
FROM snowplow_page_views
WHERE page_url_path RLIKE '.*wiki/.*' LIMIT 100
)

, snippet_viewed AS (

  SELECT
    user_snowplow_domain_id,
    TO_DATE(page_view_start),
    page_url_path,
    'snippets_viewed'
  FROM snowplow_page_views
  WHERE page_url_path RLIKE '.*/snippets/[0-9].*' LIMIT 100

)

, snippet_edited AS (
  SELECT
    user_snowplow_domain_id,
    TO_DATE(page_view_start),
    page_url_path,
    'snippet_edited'
  FROM snowplow_page_views
  WHERE page_url_path RLIKE '.*/snippets/[0-9]*/edit' LIMIT 100
)

, snippet_created AS (
  SELECT
    user_snowplow_domain_id,
    TO_DATE(page_view_start),
    page_url_path,
    'snippet_created'
  FROM snowplow_page_views
  WHERE page_url_path RLIKE '.*/snippets/[0-9]*/edit' LIMIT 100
)


, search_performed AS (

  SELECT
    user_snowplow_domain_id,
    TO_DATE(page_view_start),
    page_url_path,
    'search_performed'

FROM snowplow_page_views
WHERE page_url_path RLIKE '.*/search' LIMIT 100

)

SELECT
*
FROM search_performed
