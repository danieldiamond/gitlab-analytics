WITH zuora_product AS (
    SELECT *
    FROM {{ ref('zuora_product_source') }}

), zuora_product_rate_plan AS (

SELECT *
FROM {{ ref('zuora_product_rate_plan_source') }}
    )



select zuora_product.*,
       zuora_product_rate_plan_charge.*


from ANALYTICS.ANALYTICS_STAGING.ZUORA_PRODUCT_SOURCe as zuora_product
 left join ANALYTICS.MSENDAL_SCRATCH_STAGING.zuora_product_rate_plan_charge_source as zuora_product_rate_plan_charge
 on zuora_product_rate_plan_charge.product_id = product_id.product_id