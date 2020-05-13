WITH zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}

), zuora_product_rate_plan AS (
    SELECT *
    FROM {{ ref('zuora_product_rate_plan_source') }}
)

SELECT DISTINCT
  zuora_product.product_id,
  zuora_product.product_name,
  zuora_product.sku,
  zuora_product.category,
  zuora_product_rate_plan.product_rate_plan_name like '%reporter_access%'   AS is_reporter_license
FROM zuora_product
  LEFT JOIN zuora_product_rate_plan ON zuora_product_rate_plan.product_id = zuora_product.product_id
WHERE zuora_product.is_deleted = FALSE


