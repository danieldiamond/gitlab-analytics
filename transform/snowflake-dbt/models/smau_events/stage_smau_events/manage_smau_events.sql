WITH manage_snowplow_smau_pageviews_events AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id::NUMBER     AS gitlab_user_id,
    event_date,
    event_type,
    event_surrogate_key,
    'snowplow_pageviews'       AS source_type

  FROM {{ ref('manage_snowplow_smau_pageviews_events')}}

)

, manage_gitlab_dotcom_smau_events AS (

  SELECT
    NULL               AS user_snowplow_domain_id,
    user_id::NUMBER    AS gitlab_user_id,
    event_date,
    event_type,
    event_surrogate_key,
    'gitlab_backend'   AS source_type

  FROM {{ ref('manage_gitlab_dotcom_smau_events')}}

)

, unioned AS (

    SELECT *
    FROM manage_snowplow_smau_pageviews_events

    UNION

    SELECT *
    FROM manage_gitlab_dotcom_smau_events

)

SELECT *
FROM unioned
