WITH renamed AS (
  SELECT DISTINCT
    id                                   AS customer_id,
    created_at                           AS customer_created_at,
    updated_at                           AS customer_updated_at,
    sign_in_count,
    current_sign_in_at,
    last_sign_in_at,
    zuora_account_id,
    salesforce_account_id                AS sfdc_account_id,
    billable,
    DENSE_RANK() OVER (
      PARTITION BY customer_id
      ORDER BY customer_updated_at DESC) AS row_number
    --first_name,
    --last_name,
    --email,
    --reset_password_sent_at,
    --remember_created_at,
    --current_sign_in_ip,
    --last_sign_in_ip,
    --provider,
    --uid,
    --country,
    --state,
    --city,
    --zip_code,
    --vat_code,
    --company,
    --confirmed_at,
    --confirmation_sent_at,
 FROM {{ source('customers', 'customers_db_customers') }}
)

SELECT *
FROM renamed
WHERE row_number = 1
