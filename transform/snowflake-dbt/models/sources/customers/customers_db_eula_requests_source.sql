WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_eula_requests') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                AS eula_request_id,
      eula_id::NUMBER           AS eula_id,
      customer_id::NUMBER       AS customer_id,
      subscription_id::VARCHAR   AS zuora_subscription_id,
      subscription_name::VARCHAR AS zuora_subscription_name,
      eula_type::NUMBER         AS eula_type,
      accepted_at::TIMESTAMP     AS accepted_at,
      created_at::TIMESTAMP      AS created_at,
      updated_at::TIMESTAMP      AS updated_at
    FROM source

)

SELECT *
FROM renamed
