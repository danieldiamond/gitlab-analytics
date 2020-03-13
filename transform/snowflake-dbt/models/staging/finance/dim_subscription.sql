WITH zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan') }}

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge') }}

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription') }}

)

SELECT
      zuora_account.account_id,
      zuora_account.crm_id,
      zuora_subscription.subscription_id,
      zuora_subscription.subscription_name_slugify,
      zuora_subscription.subscription_status,
      zuora_subscription.version                          AS subscription_version,
      zuora_rate_plan_charge.rate_plan_charge_id,
      zuora_rate_plan_charge.rate_plan_charge_number,
      zuora_rate_plan_charge.rate_plan_charge_name,
      zuora_rate_plan_charge.segment                      AS rate_plan_charge_segment,
      zuora_rate_plan_charge.version                      AS rate_plan_charge_version,
      zuora_rate_plan_charge.effective_start_date::DATE   AS effective_start_date,
      zuora_rate_plan_charge.effective_end_date::DATE     AS effective_end_date,
      zuora_rate_plan_charge.unit_of_measure,
      zuora_rate_plan_charge.quantity,
      zuora_rate_plan_charge.mrr,
      zuora_product.product_name
    FROM zuora_account
    INNER JOIN zuora_subscription
      ON zuora_account.account_id = zuora_subscription.account_id
    INNER JOIN zuora_rate_plan
      ON zuora_subscription.subscription_id = zuora_rate_plan.subscription_id
    INNER JOIN zuora_rate_plan_charge
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    LEFT JOIN zuora_product
      ON zuora_rate_plan_charge.product_id = zuora_product.product_id
