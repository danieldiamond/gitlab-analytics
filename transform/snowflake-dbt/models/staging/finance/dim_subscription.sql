WITH zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}

)

SELECT
      zuora_account.account_id,
      zuora_account.crm_id,
       zuora_account.sfdc_oppotunity_id,
      zuora_subscription.subscription_id,
      zuora_subscription.subscription_name_slugify,
      zuora_subscription.subscription_status,
      zuora_subscription.version                          AS subscription_version,
      zuora_subscription.auto_renawal as is_auto_renewal,
      zuora_subscription.renewal_setting,
       zuora_subscription.zuora_renewal_subscription_slugify,
       zuora_subscription.renewal_term,
       zuora_subscription.renewal_term_period_type,
       zuora_subscription.quote_type,
       zuora_subscription.renewal_setting
    FROM zuora_account
    INNER JOIN zuora_subscription
      ON zuora_account.account_id = zuora_subscription.account_id
