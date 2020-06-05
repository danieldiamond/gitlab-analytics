WITH usage_data AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_usage_data_events') }}

)

, aggregated AS (

    SELECT
      {{ dbt_utils.star(from=ref('gitlab_dotcom_usage_data_events'), except=["EVENT_CREATED_AT", "PARENT_TYPE", "PARENT_ID", "PARENT_CREATED_AT"]) }},
      TO_DATE(event_created_at) AS event_date,
      COUNT(*)                  AS event_count
    FROM usage_data
    {{ dbt_utils.group_by(n=16) }}

)

SELECT *
FROM aggregated
