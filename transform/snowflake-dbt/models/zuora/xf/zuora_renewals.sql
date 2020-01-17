WITH zuora_subscription AS (
  
  SELECT *
  FROM {{ ref('zuora_subscription') }}
  
)

, transform AS (

    SELECT 
      subscription_name,
      subscription_name_slugify,    
      subscription_status, 
      version, 
      subscription_start_date, 
      subscription_end_date, 
      LAG(subscription_end_date , 1) 
        OVER (PARTITION BY subscription_name_slugify 
              ORDER BY version DESC) AS next_version_subscription_end_date
    FROM zuora_subscription
    
)

SELECT *
FROM transform
WHERE subscription_end_date = next_version_subscription_end_date
  OR next_version_subscription_end_date IS NULL
