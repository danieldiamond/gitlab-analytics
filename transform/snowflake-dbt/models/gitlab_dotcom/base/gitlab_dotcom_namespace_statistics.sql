WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'namespace_statistics') }}

), renamed AS (

    SELECT

      id :: integer                                      as namespace_statistics_id,
      namespace_id :: integer                            as namespace_id,
      shared_runners_seconds :: integer                  as shared_runners_seconds,
      shared_runners_seconds_last_reset :: timestamp     as shared_runners_seconds_last_reset

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
