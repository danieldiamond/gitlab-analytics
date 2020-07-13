{{
  config( materialized='ephemeral')
}}

WITH zuora_subscription_snapshots AS (

  SELECT *
  FROM {{ ref('zuora_subscription_snapshots_source') }}
  WHERE '{{ var('valid_at') }}'::DATE >= dbt_valid_from
    AND '{{ var('valid_at') }}'::DATE < {{ coalesce_to_infinity('dbt_valid_to') }}

), zuora_subscription AS (

  SELECT *
  FROM {{ ref('zuora_subscription_source') }}

), zuora_account AS (

  SELECT
    account_id,
    crm_id
  FROM {{ ref('zuora_account_snapshots_source') }}
  WHERE '{{ var('valid_at') }}'::DATE >= dbt_valid_from
    AND '{{ var('valid_at') }}'::DATE < {{ coalesce_to_infinity('dbt_valid_to') }}

)

SELECT
  zuora_subscription_snapshots.subscription_id,
  zuora_account.crm_id,
  zuora_account.account_id,
  zuora_subscription_snapshots.subscription_name_slugify,
  zuora_subscription_snapshots.subscription_status,
  zuora_subscription_snapshots.version                                                AS subscription_version,
  zuora_subscription_snapshots.auto_renew                                             AS is_auto_renew,
  zuora_subscription_snapshots.zuora_renewal_subscription_name,
  zuora_subscription_snapshots.zuora_renewal_subscription_name_slugify,
  zuora_subscription_snapshots.renewal_term,
  zuora_subscription_snapshots.renewal_term_period_type,
  zuora_subscription_snapshots.subscription_start_date                                AS subscription_start_date,
  zuora_subscription_snapshots.subscription_end_date                                  AS subscription_end_date,
  DATE_TRUNC('month', zuora_subscription.subscription_start_date)           AS subscription_start_month,
  DATE_TRUNC('month', zuora_subscription.subscription_end_date)             AS subscription_end_month
FROM zuora_subscription_snapshots
INNER JOIN zuora_subscription
  ON zuora_subscription.subscription_id = zuora_subscription_snapshots.subscription_id
INNER JOIN zuora_account
  ON zuora_account.account_id = zuora_subscription.account_id
WHERE zuora_subscription_snapshots.is_deleted = FALSE
  AND exclude_from_analysis IN ('False', '')
