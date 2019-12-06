{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('customers', 'customers_db_customers') }}

), renamed AS (

  SELECT DISTINCT
    id::INTEGER                      AS customer_id,
    created_at::TIMESTAMP            AS customer_created_at,
    updated_at::TIMESTAMP            AS customer_updated_at,
    --sign_in_count // missing from manifest, issue 1860,
    current_sign_in_at::TIMESTAMP    AS current_sign_in_at,
    last_sign_in_at::TIMESTAMP       AS last_sign_in_at,
    --current_sign_in_ip,
    --last_sign_in_ip,
    provider::VARCHAR                AS customer_provider,
    NULLIF(uid, '')::VARCHAR         AS customer_provider_user_id,
    zuora_account_id::VARCHAR        AS zuora_account_id,
    country::VARCHAR                 AS country,
    state::VARCHAR                   AS state,
    city::VARCHAR                    AS city,
    company::VARCHAR                 AS company,
    salesforce_account_id::VARCHAR   AS sfdc_account_id,
    billable::BOOLEAN                AS customer_is_billable,
    access_token::VARCHAR            AS access_token,
    confirmation_token::VARCHAR      AS confirmation_token,
    confirmed_at::TIMESTAMP          AS confirmed_at,
    confirmation_sent_at::TIMESTAMP  AS confirmation_sent_at
 FROM source
 WHERE rank_in_key = 1
)

SELECT *
FROM renamed
