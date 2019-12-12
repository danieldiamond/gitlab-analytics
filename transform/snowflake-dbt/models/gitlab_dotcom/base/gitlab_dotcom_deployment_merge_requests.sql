{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'deployment_merge_requests') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pkey ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      deployment_id::INTEGER                           AS deployment_id,
      merge_request_id::INTEGER                        AS merge_request_id
      pkey::INTEGER                                    AS pkey
    FROM source

)

SELECT *
FROM renamed
