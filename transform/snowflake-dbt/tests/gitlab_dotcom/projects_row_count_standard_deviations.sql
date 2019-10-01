WITH data AS (

    SELECT
      project_id            AS unique_id,
      project_created_at    AS timestamp_column
    FROM {{ref('gitlab_dotcom_projects')}}

)

{{row_count_standard_deviations(min_stddevs=5)}}
