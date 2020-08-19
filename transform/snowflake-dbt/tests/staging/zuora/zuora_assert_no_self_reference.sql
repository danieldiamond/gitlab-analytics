-- Asserts that a subscription is not linked to itself as a renewal subscription

SELECT
    *
FROM {{ ref('zuora_subscription_xf') }},
    LATERAL flatten(input => zuora_renewal_subscription_name_slugify, OUTER => TRUE) renewal
WHERE renewal.value :: STRING = subscription_name_slugify
