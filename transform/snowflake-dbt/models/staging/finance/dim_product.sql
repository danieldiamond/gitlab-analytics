WITH zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}

), zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}

)

SELECT
    zuora_product.product_id,
    zuora_product.product_name,
    {{delivery('zuora_product.category')}},
    zuora_product.sku,
    zuora_product.category
FROM zuora_product
WHERE zuora_product.is_deleted = FALSE


