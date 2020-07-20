{{
  config( materialized='ephemeral')
}}

WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_snapshots_source') }}
    WHERE '{{ var('valid_at') }}'::TIMESTAMP_TZ >= dbt_valid_from
      AND '{{ var('valid_at') }}'::TIMESTAMP_TZ < {{ coalesce_to_infinity('dbt_valid_to') }}

), zuora_contact AS (

    SELECT *
    FROM {{ ref('zuora_contact_snapshots_source') }}
    WHERE '{{ var('valid_at') }}'::TIMESTAMP_TZ >= dbt_valid_from
      AND '{{ var('valid_at') }}'::TIMESTAMP_TZ < {{ coalesce_to_infinity('dbt_valid_to') }}

), excluded_accounts AS (

    SELECT DISTINCT
      account_id
    FROM {{ref('zuora_excluded_accounts')}}

)

SELECT
  zuora_account.account_id,
  zuora_account.crm_id,
  zuora_account.account_number,
  zuora_account.account_name,
  zuora_account.status          AS account_status,
  zuora_account.parent_id,
  zuora_account.sfdc_account_code,
  zuora_account.currency        AS account_currency,
  zuora_contact.country         AS sold_to_country,
  zuora_account.is_deleted,
  zuora_account.account_id IN (
                                SELECT
                                  account_id
                                FROM excluded_accounts
                              ) AS is_excluded
FROM zuora_account
LEFT JOIN zuora_contact
  ON COALESCE(zuora_account.sold_to_contact_id, zuora_account.bill_to_contact_id) = zuora_contact.contact_id
