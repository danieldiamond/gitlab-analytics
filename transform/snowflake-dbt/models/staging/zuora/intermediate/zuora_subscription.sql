-- depends_on: {{ ref('zuora_excluded_accounts') }}

WITH source AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
  AND exclude_from_analysis IN ('False', '')
  AND account_id NOT IN ({{ zuora_excluded_accounts() }})
  AND subscription_id != '2c92a0fe70cd67e30170e4bbe498463e' -- https://gitlab.com/gitlab-data/analytics/-/issues/4076
