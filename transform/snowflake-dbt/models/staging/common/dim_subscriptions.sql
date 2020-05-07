WITH zuora_subscription AS (
  SELECT *
  FROM {{ ref('zuora_subscription_source') }}

), zuora_account AS (
  SELECT
    account_id,
    crm_id
  FROM {{ ref('zuora_account_source') }}
)

SELECT
  zuora_subscription.subscription_id,
  zuora_account.crm_id,
  zuora_subscription.subscription_name_slugify,
  zuora_subscription.subscription_status,
  zuora_subscription.version                                    AS subscription_version,
  zuora_subscription.auto_renew                                 AS is_auto_renew,
  zuora_subscription.zuora_renewal_subscription_name,
  zuora_subscription.zuora_renewal_subscription_name_slugify,
  zuora_subscription.renewal_term,
  zuora_subscription.renewal_term_period_type,
  zuora_subscription.subscription_start_date,
  zuora_subscription.subscription_end_date
FROM zuora_subscription
INNER JOIN zuora_account ON zuora_account.account_id = zuora_subscription.account_id
WHERE is_deleted = FALSE
  AND exclude_from_analysis IN ('False', '')

