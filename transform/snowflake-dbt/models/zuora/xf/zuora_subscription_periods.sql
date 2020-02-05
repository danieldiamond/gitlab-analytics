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
      term_start_date                 AS subscription_version_term_start_date,
      term_end_date                   AS subscription_version_term_end_date, 
      MIN(term_start_date) 
      OVER (PARTITION BY subscription_name_slugify 
            ORDER BY zuora_subscription.version DESC 
            ROWS BETWEEN UNBOUNDED PRECEDING
                   AND 1 PRECEDING)   AS min_following_subscription_version_term_start_date
    FROM zuora_subscription
    INNER JOIN zuora_account
      ON zuora_subscription.account_id = zuora_account.account_id
    
)

, subscription_with_valid_auto_renew_setting AS (
  
  
    /* 
    specific CTE to check auto_renew settings before the renewal happens
    special case for auto-reneweable subscription with a failing payment
    good example is subscription_name_slugify = 'a-s00014110'
    */
    SELECT DISTINCT 
        subscription_name_slugify,
        term_start_date,
        term_end_date,
        FIRST_VALUE(auto_renew) 
          OVER 
            (PARTITION BY subscription_name_slugify, 
                          term_start_date,
                          term_end_date
             ORDER BY version DESC) AS last_auto_renew
    FROM zuora_subscription
    /* 
    when subscription with auto-renew turned on, but CC declined
     a new version of the same subscription is created (same term_end_date)
     created_date eis after the term_end_date 
     this new version has auto_column set to FALSE
     */
    WHERE created_date < term_end_date 
  
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
  subscription_joined_with_charges.*,
  COALESCE(subscription_with_valid_auto_renew_setting.last_auto_renew, 
    FALSE)                                AS has_auto_renew_on,
  CASE
    -- manual linked subscription
    WHEN subscription_joined_with_charges.zuora_renewal_subscription_name_slugify IS NOT NULL THEN TRUE
    -- new version available, got renewed
    WHEN LEAD(subscription_joined_with_charges.subscription_name_slugify) 
          OVER (
            PARTITION BY subscription_joined_with_charges.subscription_name_slugify 
            ORDER BY version
          ) IS NOT NULL
      THEN TRUE
    ELSE FALSE
  END                                     AS is_renewed
FROM subscription_joined_with_charges
LEFT JOIN subscription_with_valid_auto_renew_setting
  ON subscription_joined_with_charges.subscription_name_slugify = subscription_with_valid_auto_renew_setting.subscription_name_slugify
  AND subscription_joined_with_charges.subscription_version_term_start_date = subscription_with_valid_auto_renew_setting.term_start_date
  AND subscription_joined_with_charges.subscription_version_term_end_date = subscription_with_valid_auto_renew_setting.term_end_date
ORDER BY subscription_start_date, version
