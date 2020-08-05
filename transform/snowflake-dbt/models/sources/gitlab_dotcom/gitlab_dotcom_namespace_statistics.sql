WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'namespace_statistics') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT

      id::NUMBER                                      AS namespace_statistics_id,
      namespace_id::NUMBER                            AS namespace_id,
      shared_runners_seconds::NUMBER                  AS shared_runners_seconds,
      shared_runners_seconds_last_reset::TIMESTAMP     AS shared_runners_seconds_last_reset

    FROM source

)

SELECT *
FROM renamed
