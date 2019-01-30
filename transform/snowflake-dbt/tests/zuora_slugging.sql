-- This looks for commas or periods in the slug name 

SELECT subscription_name_slugify
FROM {{ref('zuora_subscription')}}
WHERE subscription_name_slugify LIKE '%,%'
OR subscription_name_slugify LIKE '%.%'

