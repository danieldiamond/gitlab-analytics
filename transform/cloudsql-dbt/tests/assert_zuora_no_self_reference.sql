-- Asserts that a subscription is not linked to itself as a renewal subscription

SELECT
    *
FROM { { ref('zuora_subscription_xf') } }
WHERE zuora_renewal_subscription_name_slugify = subscription_name_slugify
