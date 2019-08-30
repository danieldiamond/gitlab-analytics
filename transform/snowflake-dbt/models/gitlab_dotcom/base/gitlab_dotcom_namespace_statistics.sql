{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'namespace_statistics') }}

), renamed AS (

    SELECT

      id :: integer                                      AS namespace_statistics_id,
      namespace_id :: integer                            AS namespace_id,
      shared_runners_seconds :: integer                  AS shared_runners_seconds,
      shared_runners_seconds_last_reset :: timestamp     AS shared_runners_seconds_last_reset

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
