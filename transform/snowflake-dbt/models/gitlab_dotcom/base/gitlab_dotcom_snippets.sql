{{ config({
    "materialized": "incremental",
    "unique_key": "snippet_id",
    "schema": "sensitive"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'snippets') }}
    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (
  
    SELECT
      id::INTEGER               AS snippet_id,
      author_id::INTEGER        AS author_id,
      project_id::INTEGER       AS project_id,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at,
      type::VARCHAR             AS snippet_type,
      file_name::VARCHAR        AS file_name,
      visibility_level::INTEGER AS visibility_level
      
    FROM source
    
)

SELECT * 
FROM renamed
