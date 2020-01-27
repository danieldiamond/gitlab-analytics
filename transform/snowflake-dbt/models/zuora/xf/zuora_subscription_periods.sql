WITH zuora_account AS (
  
  SELECT *
  FROM {{ ref('zuora_account') }}
  
)

, zuora_rate_plan AS (
  
  SELECT *
  FROM {{ ref('zuora_rate_plan') }}
  
)

, zuora_rate_plan_charge AS (
  
  SELECT *
  FROM {{ ref('zuora_rate_plan_charge') }}
  
)

, zuora_subscription AS (
  
  SELECT *
  FROM {{ ref('zuora_subscription') }}
  
)

, subscription_joined_with_accounts AS (

    SELECT DISTINCT
      zuora_subscription.subscription_id,
      zuora_subscription.subscription_name,
      zuora_subscription.subscription_name_slugify,    
      zuora_subscription.subscription_status, 
      zuora_subscription.version,
      zuora_subscription.zuora_renewal_subscription_name_slugify,
      zuora_subscription.renewal_term,
      zuora_subscription.renewal_term_period_type,
      
      zuora_subscription.account_id, 
      zuora_account.account_number,
      zuora_account.account_name,
      subscription_start_date, 
      term_end_date                   AS subscription_version_term_end_date, 
      term_start_date                 AS subscription_version_term_start_date,
      MIN(term_start_date) 
      OVER (PARTITION BY subscription_name_slugify 
            ORDER BY zuora_subscription.version DESC 
            ROWS BETWEEN UNBOUNDED PRECEDING
                   AND 1 PRECEDING)   AS min_following_subscription_version_term_start_date
    FROM zuora_subscription
    INNER JOIN zuora_account
      ON zuora_subscription.account_id = zuora_account.account_id
    
)

, subscription_joined_with_charges AS (
  
    SELECT DISTINCT
      subscription_joined_with_accounts.subscription_id,
      subscription_joined_with_accounts.subscription_name,
      subscription_joined_with_accounts.subscription_name_slugify,    
      subscription_joined_with_accounts.subscription_status, 
      subscription_joined_with_accounts.version,
      subscription_joined_with_accounts.zuora_renewal_subscription_name_slugify,
      subscription_joined_with_accounts.account_id, 
      subscription_joined_with_accounts.account_number,
      subscription_joined_with_accounts.account_name,
      subscription_joined_with_accounts.subscription_start_date, 
      subscription_joined_with_accounts.subscription_version_term_start_date,
      subscription_joined_with_accounts.subscription_version_term_end_date,
      LAST_VALUE(mrr) OVER (PARTITION BY subscription_joined_with_accounts.subscription_id 
        ORDER BY zuora_rate_plan_charge.effective_start_date)          AS mrr,
      SUM(tcv) OVER (
        PARTITION BY subscription_joined_with_accounts.subscription_id) AS tcv
    FROM subscription_joined_with_accounts
    INNER JOIN zuora_rate_plan
      ON subscription_joined_with_accounts.subscription_id = zuora_rate_plan.subscription_id
    INNER JOIN zuora_rate_plan_charge
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
        -- remove refunded subscriptions
        AND mrr > 0 
        AND tcv > 0
    WHERE (subscription_version_term_start_date  < min_following_subscription_version_term_start_date
      OR min_following_subscription_version_term_start_date IS NULL)
      -- remove cancelled subscription
      AND subscription_version_term_end_date != subscription_version_term_start_date
      
)

SELECT 
  *,
  CASE
    -- manual linked subscription
    WHEN zuora_renewal_subscription_name_slugify IS NOT NULL THEN TRUE
    -- new version available, got renewed
    WHEN LEAD(subscription_name_slugify) OVER (PARTITION BY subscription_name_slugify ORDER BY version) IS NOT NULL
      THEN TRUE
    ELSE FALSE
  END AS is_renewed
FROM subscription_joined_with_charges
ORDER BY subscription_start_date
