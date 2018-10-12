-- Ensures that the months since the start of the cohort can never be less than 0.
SELECT *
FROM {{ ref('zuora_mrr_total') }}
WHERE months_since_cohort_start < 0