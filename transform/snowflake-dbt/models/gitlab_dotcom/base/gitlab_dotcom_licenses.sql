-- disabled model until the data starts flowing in (the source table is missing from tap_postgres)

{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'licenses') }}

), renamed AS (

    SELECT

      id :: integer                                 as license_id,
      created_at :: timestamp                       as license_created_at,
      updated_at :: timestamp                       as license_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed