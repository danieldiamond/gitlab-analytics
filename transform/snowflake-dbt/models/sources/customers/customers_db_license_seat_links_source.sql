WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_license_seat_links') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY zuora_subscription_id, report_date ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      zuora_subscription_id::VARCHAR     AS zuora_subscription_id,
      zuora_subscription_name::VARCHAR   AS zuora_subscription_name,
      order_id::INTEGER                  AS order_id,
      report_date::DATE                  AS report_date,
      license_starts_on::DATE            AS license_starts_on,
      created_at::TIMESTAMP              AS created_at,
      updated_at::TIMESTAMP              AS updated_at,
      active_user_count::INTEGER         AS active_user_count,
      license_user_count::INTEGER        AS license_user_count,
      max_historical_user_count::INTEGER AS max_historical_user_count
    FROM source  

)

SELECT *
FROM renamed