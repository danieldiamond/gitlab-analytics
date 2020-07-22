WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'repository_languages') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY project_programming_language_id ORDER BY _uploaded_at DESC) = 1

), renamed AS (
  
    SELECT
      MD5(project_programming_language_id)::VARCHAR AS repository_language_id,
      project_id::INTEGER                           AS project_id,
      programming_language_id::INTEGER              AS programming_language_id,
      share::FLOAT                                  AS share
    FROM source
    
)

SELECT * 
FROM renamed
