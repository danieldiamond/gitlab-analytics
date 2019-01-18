-- Ensures that the months since the start of the cohort can never be less than 0.
SELECT *
FROM {{ ref('zuora_mrr_totals') }}
WHERE MONTHS_SINCE_ZUORA_SUBSCRIPTION_COHORT_START < 0