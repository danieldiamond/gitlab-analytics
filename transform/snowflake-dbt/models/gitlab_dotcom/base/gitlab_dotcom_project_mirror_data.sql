WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'project_mirror_data') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1


), renamed AS (

    SELECT

      id::INTEGER                                     AS project_mirror_data_id,
      project_id::INTEGER                             AS project_id,
      retry_count::INTEGER                            AS retry_count,
      last_update_started_at::TIMESTAMP               AS last_update_started_at,
      last_update_scheduled_at::TIMESTAMP             AS last_update_scheduled_at,
      next_execution_timestamp::TIMESTAMP             AS next_execution_timestamp

    FROM source

)

SELECT *
FROM renamed
