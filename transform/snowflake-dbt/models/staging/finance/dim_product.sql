WITH zuora_product AS (
    SELECT *
    FROM {{ ref('zuora_product_source') }}

), zuora_product_rate_plan_charge AS (

SELECT *
FROM {{ ref('zuora_product_rate_plan_charge_source') }}
    )



select zuora_product.product_id,
       zuora_product.product_name,
       zuora_product.sku,
       zuora_product.category,
       zuora_product_rate_plan_charge.accounting_code,
       zuora_product_rate_plan_charge.charge_type,
       zuora_product_rate_plan_charge.product_rate_plan_charge_description,
       zuora_product_rate_plan_charge.product_rate_plan_charge_name,
       zuora_product_rate_plan_charge.revenue_recognition_rule_name


from zuora_product as zuora_product
         left join zuora_product_rate_plan_charge as zuora_product_rate_plan_charge
                   on zuora_product_rate_plan_charge.product_id = zuora_product.product_id
where zuora_product.is_deleted = False
