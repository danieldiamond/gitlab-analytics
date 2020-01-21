WITH zuora_subscription AS (
  
  SELECT *
  FROM {{ ref('zuora_subscription') }}
  
)

, zuora_account AS (
  
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

, transform AS (

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
      subscription_end_date           AS subscription_version_end_date, 
      term_end_date                   AS subscription_version_term_end_date, 
      term_start_date                 AS subscription_version_term_start_date,
        MIN(term_start_date) 
        OVER (PARTITION BY subscription_name_slugify 
              ORDER BY zuora_subscription.version DESC 
              ROWS BETWEEN UNBOUNDED PRECEDING
                     AND 1 PRECEDING) AS min_following_subscription_version_term_start_date
    FROM zuora_subscription
    LEFT JOIN zuora_account
      ON zuora_subscription.account_id = zuora_account.account_id
    
)

, last_cte AS (
SELECT DISTINCT
  transform.subscription_id,
  transform.subscription_name,
  transform.subscription_name_slugify,    
  transform.subscription_status, 
  transform.version,
  transform.zuora_renewal_subscription_name_slugify,
  transform.account_id, 
  transform.account_number,
  transform.account_name,
  transform.subscription_start_date, 
  transform.subscription_version_end_date,
  transform.subscription_version_term_end_date,
  transform.subscription_version_term_start_date
  
FROM transform
INNER JOIN zuora_rate_plan
  ON transform.subscription_id = zuora_rate_plan.subscription_id
INNER JOIN zuora_rate_plan_charge
  ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    AND mrr > 0 AND tcv > 0
WHERE (subscription_version_term_start_date  < min_following_subscription_version_term_start_date
  OR min_following_subscription_version_term_start_date IS NULL)
  AND subscription_status <> 'Cancelled'
)

SELECT 
  *,
  CASE
    WHEN zuora_renewal_subscription_name_slugify IS NOT NULL THEN TRUE
    WHEN LAG(subscription_name_slugify, -1) OVER (PARTITION BY subscription_name_slugify ORDER BY version DESC) IS NOT NULL
      THEN TRUE
    ELSE FALSE
  END AS is_renewed
FROM last_cte
