WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}

), zuora_contact AS (

    SELECT *
    FROM {{ ref('zuora_contact_source') }}

), excluded_accounts AS (

    SELECT DISTINCT account_id
    FROM {{ref('zuora_excluded_accounts')}}
), sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}

)


SELECT
    zuora_account.account_id,
    zuora_account.crm_id,
    sfdc_account.account_id AS customer_id,
    zuora_account.account_number,
    zuora_account.account_name,
    zuora_account.status     AS account_status,
    zuora_account.parent_id,
    zuora_account.sfdc_account_code,
    zuora_account.currency   AS account_currency,
    zuora_contact.country,
    zuora_account.is_deleted,
    zuora_account.account_id IN (SELECT account_id FROM excluded_accounts) AS is_excluded
FROM zuora_account
     LEFT JOIN zuora_contact
    ON COALESCE(zuora_account.sold_to_contact_id, zuora_account.bill_to_contact_id) = zuora_contact.contact_id
    LEFT JOIN sfdc_account
    ON sfdc_account.account_id = zuora_account.crm_id