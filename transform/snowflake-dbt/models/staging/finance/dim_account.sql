WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}

), zuora_contact AS (

    SELECT *
    FROM {{ ref('zuora_contact_source') }}

), excluded_accounts AS (

    SELECT *
    FROM {{zuora_excluded_accounts()}}

    )

SELECT
    zuora_account.account_id,
    zuora_account.account_number,
    zuora_account.account_name,
    zuora_account.status     AS account_status,
    zuora_account.crm_id,
    zuora_account.parent_id,
    zuora_account.sfdc_account_code,
    zuora_account.currency   AS account_currency,
    zuora_contact.first_name AS contact_first_name,
    zuora_contact.last_name  AS contact_last_name,
    zuora_contact.country
FROM zuora_account
     LEFT JOIN zuora_contact
    ON COALESCE(zuora_account.sold_to_contact_id, zuora_account.bill_to_contact_id) = zuora_contact.contact_id
WHERE zuora_account.is_deleted = FALSE
  AND zuora_account.account_id NOT IN ( 
                SELECT account_id
                FROM excluded_accounts
                WHERE NOT is_permanently_excluded)
