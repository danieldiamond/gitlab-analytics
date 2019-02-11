WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.merge_request_metrics


), renamed AS (

    SELECT

      id :: integer                                              as merge_request_metric_id,
      merge_request_id :: integer                                as merge_request_id,

      TRY_CAST(latest_build_started_at as timestamp)             as latest_build_started_at,
      TRY_CAST(latest_build_finished_at as timestamp)            as latest_build_finished_at,
      TRY_CAST(first_deployed_to_production_at as timestamp)     as first_deployed_to_production_at,
      TRY_CAST(merged_at as timestamp)                           as merged_at,
      TRY_CAST(created_at as timestamp)                          as merge_request_metric_created_at,
      TRY_CAST(updated_at as timestamp)                          as merge_request_metric_updated_at,
      TRY_CAST(latest_closed_at as timestamp)                    as latest_closed_at,

      TRY_CAST(pipeline_id as integer)                           as pipeline_id,
      TRY_CAST(merged_by_id as integer)                          as merged_by_id,
      TRY_CAST(latest_closed_by_id as integer)                   as latest_closed_by_id,

      TO_TIMESTAMP(_updated_at :: integer)                       as merge_request_metrics_last_updated_at

    FROM source


)

SELECT *
FROM renamed
