WITH zuora_subscription AS (
  
  SELECT *
  FROM {{ ref('zuora_subscription') }}
  
)

, zuora_account AS (
  
  SELECT *
  FROM {{ ref('zuora_account') }}
  
)

, transform AS (

    SELECT 
      zuora_subscription.subscription_name,
      zuora_subscription.subscription_name_slugify,    
      zuora_subscription.subscription_status, 
      zuora_subscription.version,
      
      zuora_subscription.account_id, 
      zuora_account.account_number,
      zuora_account.account_name,
      subscription_start_date, 
      subscription_end_date, 
      LAG(subscription_end_date , 1) 
        OVER (PARTITION BY subscription_name_slugify 
              ORDER BY version DESC) AS next_version_subscription_end_date
    FROM zuora_subscription
    LEFT JOIN zuora_account
      ON zuora_subscription.account_id = zuora_account.account_id
    
)

SELECT 
  subscription_name,
  subscription_name_slugify,    
  subscription_status, 
  version,
  account_id, 
  account_number,
  account_name,
  subscription_start_date, 
  subscription_end_date
FROM transform
WHERE subscription_end_date <> next_version_subscription_end_date
  OR next_version_subscription_end_date IS NULL
