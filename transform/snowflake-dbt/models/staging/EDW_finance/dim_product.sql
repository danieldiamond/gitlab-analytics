WITH zuora_product AS (
    SELECT *
    FROM {{ ref('zuora_product_source') }}

), zuora_product_rate_plan AS (

SELECT *
FROM {{ ref('zuora_product_rate_plan_source') }}
    )
