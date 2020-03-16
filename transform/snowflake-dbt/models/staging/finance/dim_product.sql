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
       zuora_product.category,
       {{ product_category('zuora_rate_plan.rate_plan_name') }}
from  zuora_product
         inner join zuora_rate_plan
                   on zuora_rate_plan.product_id = zuora_product.product_id
where zuora_product.is_deleted = False
