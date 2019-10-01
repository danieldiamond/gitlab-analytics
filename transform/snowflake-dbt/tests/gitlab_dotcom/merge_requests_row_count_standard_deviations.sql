WITH data AS (

    SELECT
      merge_request_id            AS unique_id,
      merge_request_created_at    AS timestamp_column
    FROM {{ref('gitlab_dotcom_merge_requests')}}

)

{{row_count_standard_deviations(min_stddevs=5)}}
