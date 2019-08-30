-- disabled model until the data starts flowing in (the source table is missing from tap_postgres)
{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_import_data') }}

), renamed AS (

    SELECT

      id :: integer                      AS project_import_relation_id,
      project_id :: integer              AS project_id

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed