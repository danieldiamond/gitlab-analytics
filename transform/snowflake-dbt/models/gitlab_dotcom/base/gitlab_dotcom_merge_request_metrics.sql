{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'merge_request_metrics') }}


), renamed AS (

    SELECT

      id :: integer                                              AS merge_request_metric_id,
      merge_request_id :: integer                                AS merge_request_id,

      latest_build_started_at :: timestamp                       AS latest_build_started_at,
      latest_build_finished_at :: timestamp                      AS latest_build_finished_at,
      first_deployed_to_production_at :: timestamp               AS first_deployed_to_production_at,
      merged_at :: timestamp                                     AS merged_at,
      created_at :: timestamp                                    AS merge_request_metric_created_at,
      updated_at :: timestamp                                    AS merge_request_metric_updated_at,
      latest_closed_at :: timestamp                              AS latest_closed_at,

      pipeline_id :: integer                                     AS pipeline_id,
      merged_by_id :: integer                                    AS merged_by_id,
      latest_closed_by_id :: integer                             AS latest_closed_by_id

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
