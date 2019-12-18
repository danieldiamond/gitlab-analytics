SELECT 
  subscription_name_slugify
FROM {{ ref('zuora_subscription_xf') }}
GROUP BY 1
HAVING COUNT(DISTINCT subscription_name) > 1
