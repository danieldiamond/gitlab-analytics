WITH usage_data AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_usage_data_events') }}

)

, aggregated AS (

    SELECT
      {{ dbt_utils.star(from=ref('gitlab_dotcom_usage_data_events'), except=["event_created_at", "namespace_id", "namespace_created_at"]) }},
      TO_DATE(event_created_at) AS event_date,
      COUNT(*)                  AS event_count
    FROM usage_data
    {{ dbt_utils.group_by(n=20) }}

)

SELECT *
FROM aggregated
