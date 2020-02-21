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

      id::INTEGER                       AS plan_id,
      created_at::TIMESTAMP             AS created_at,
      updated_at::TIMESTAMP             AS updated_at,
      name::VARCHAR                     AS plan_name,
      title::VARCHAR                    AS plan_title,
      id IN (2,3,4)                     AS plan_is_paid

    FROM source

)

SELECT *
FROM renamed
