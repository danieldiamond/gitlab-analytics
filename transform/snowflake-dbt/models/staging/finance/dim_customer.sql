WITH zuora_account AS (
    SELECT *
    FROM {{ ref('zuora_account_source') }}

), zuora_contact AS (

SELECT *
FROM {{ ref('zuora_contact_source') }}

)
--TODO: add salesforce data

select zuora_account.account_id,
       zuora_account.account_number,
       zuora_account.ACCOUNT_NAME,
       zuora_account.status     as account_status,
       zuora_account.crm_id,
       zuora_account.parent_id,
       zuora_account.SFDC_ACCOUNT_CODE,
       zuora_account.currency as account_currency,
       zuora_contact.first_name as contact_first_name,
       zuora_contact.last_name  as contact_last_name,
       zuora_contact.country
from  zuora_account
         left join zuora_contact
                   --on zuora_account.ACCOUNT_ID = zuora_contact.ACCOUNT_ID
ON COALESCE(zuora_account.sold_to_contact_id ,zuora_contact.bill_to_contact_id) = zuora_contact.contact_id
WHERE zuora_account.is_deleted = FALSE
  AND zuora_account.account_id NOT IN ({{zuora_excluded_accounts()}})

