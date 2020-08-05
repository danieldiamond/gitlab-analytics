WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'alerts_service_data') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                AS alerts_service_data_id,
      service_id::NUMBER        AS service_id,
      created_at::TIMESTAMP_NTZ  AS created_at,
      updated_at::TIMESTAMP_NTZ  AS updated_at

    FROM source

)

SELECT *
FROM renamed
