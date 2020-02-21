{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'programming_languages') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                       AS programming_language_id,
      name::VARCHAR                     AS programming_language_name
    FROM source

)

SELECT *
FROM renamed
