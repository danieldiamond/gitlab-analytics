-- Ensures that the months since the start of the cohort can never be less than 0.
-- Filters are on accounts that finance has already been alerted need fixing
-- We only use the hash when the slugs have identifying customer info

SELECT md5(subscription_name_slugify) AS new_slug
FROM {{ ref('zuora_mrr_totals') }}
WHERE MONTHS_SINCE_ZUORA_SUBSCRIPTION_COHORT_START < 0
