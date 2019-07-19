--no direct circular subscriptions

WITH base AS (

  SELECT
    subscription_name_slugify,
    c.value::string AS zuora_renewal_subscription_name_slugify
  FROM {{ref('zuora_subscription_intermediate')}},
  lateral flatten(input =>zuora_renewal_subscription_name_slugify) C
  WHERE c.index != 0

)

SELECT *
FROM base
WHERE subscription_name_slugify = zuora_renewal_subscription_name_slugify
