WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_customers_source') }}

), renamed AS (

    SELECT DISTINCT
      customer_id,
      customer_created_at,
      customer_updated_at,
      sign_in_count,
      current_sign_in_at,
      ast_sign_in_at,
      --current_sign_in_ip,
      --last_sign_in_ip,
      customer_provider,
      customer_provider_user_id,
      zuora_account_id,
      country,
      state,
      ccity,
      vat_code,
      company,
      company_size,
      sfdc_account_id,
      customer_is_billable,
      confirmed_at,
      confirmation_sent_at
    FROM source
    
)

SELECT *
FROM renamed
