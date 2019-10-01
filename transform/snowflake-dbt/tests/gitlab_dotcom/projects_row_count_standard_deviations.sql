WITH data AS (

    SELECT
      project_id            AS unique_id,
      project_created_at    AS timestamp_column
    FROM {{ref('gitlab_dotcom_projects')}}
    --WHERE timestamp_column BETWEEN '2019-01-01' AND CURRENT_DATE()

)

{{row_count_standard_deviations(min_stddevs=5)}}
