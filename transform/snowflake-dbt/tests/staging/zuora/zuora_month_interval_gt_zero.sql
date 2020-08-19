SELECT *
FROM {{ ref('zuora_base_mrr') }}
WHERE month_interval < 0