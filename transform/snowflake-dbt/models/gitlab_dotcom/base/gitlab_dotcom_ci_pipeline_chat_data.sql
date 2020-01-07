WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_pipeline_chat_data') }}

), renamed AS (

    SELECT 
      pipeline_id::INTEGER  AS ci_pipeline_id,
      chat_name_id::INTEGER AS chat_name_id,
      response_url          AS response_url

    FROM source

)


SELECT *
FROM renamed
