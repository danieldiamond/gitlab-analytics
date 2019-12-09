WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'deployment_merge_requests') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merge_request_id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      deployment_id::INTEGER                           AS deployment_id,
      merge_request_id::INTEGER                        AS merge_request_id
    FROM source
    
)

SELECT *
FROM renamed
