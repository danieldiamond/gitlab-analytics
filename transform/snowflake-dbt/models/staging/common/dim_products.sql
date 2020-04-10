WITH zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}

)

SELECT
    zuora_product.product_id,
    zuora_product.product_name,
    zuora_product.sku,
    zuora_product.category
FROM zuora_product
WHERE zuora_product.is_deleted = FALSE


