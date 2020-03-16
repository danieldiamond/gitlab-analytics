WITH zuora_product AS (
    SELECT *
    FROM {{ ref('zuora_product_source') }}

), zuora_rate_plan AS (

SELECT *
FROM {{ ref('zuora_rate_plan_source') }}
    )



select zuora_product.product_id,
       zuora_product.product_name,
       {{delivery('zuora_product.category')}},
       zuora_product.sku,
       zuora_product.category
from  zuora_product
where zuora_product.is_deleted = False


