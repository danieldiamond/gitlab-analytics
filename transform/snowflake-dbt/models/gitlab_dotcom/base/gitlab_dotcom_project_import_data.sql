{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'project_import_data') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT

      id::INTEGER                      AS project_import_relation_id,
      project_id::INTEGER              AS project_id

    FROM source

)

SELECT *
FROM renamed
