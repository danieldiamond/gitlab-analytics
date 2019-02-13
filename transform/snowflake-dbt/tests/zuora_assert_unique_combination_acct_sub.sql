-- Asserts that there is only one entry in the combo calculation used in zuora_base_trueups

WITH combo AS (
    SELECT
      account_number,
      cohort_month,
      subscription_name,
      subscription_name_slugify,
      oldest_subscription_in_cohort
    FROM {{ ref('zuora_base_mrr') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
  subscription_name,
  count(*)
FROM combo
  GROUP BY 1
HAVING count(*) > 1