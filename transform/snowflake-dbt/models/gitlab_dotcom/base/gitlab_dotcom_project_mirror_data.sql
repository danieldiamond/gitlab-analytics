-- disabled model until the data starts flowing in (the source table is missing from tap_postgres)
WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_mirror_data') }}


), renamed AS (

    SELECT

      id :: integer                                     as project_mirror_data_id,
      project_id :: integer                             as project_id,
      retry_count :: integer                            as retry_count,
      last_update_started_at :: timestamp               as last_update_started_at,
      last_update_scheduled_at :: timestamp             as last_update_scheduled_at,
      next_execution_timestamp :: timestamp             as next_execution_timestamp



    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed