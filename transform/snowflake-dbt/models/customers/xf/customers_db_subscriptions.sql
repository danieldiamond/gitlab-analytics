WITH customers AS (
  
  SELECT * 
  FROM {{ ref('customers_db_customers') }}
  
)

, orders AS (
  
  SELECT * 
  FROM {{ ref('customers_db_orders') }}
  
)

, zuora_rp AS (
  
  SELECT *
  FROM {{ ref('zuora_rate_plan')}}
  
)

, zuora_rpc AS (
  
  SELECT *
  FROM {{ ref('zuora_rate_plan_charge')}}
  
)

, zuora_subscription_xf AS (
  
  SELECT *
  FROM {{ ref('zuora_subscription_xf')}}
  
)

, joined AS (
  
  SELECT DISTINCT
    orders.subscription_name_slugify,
    zuora_subscription_xf.subscription_start_date,
    zuora_subscription_xf.oldest_subscription_in_cohort,
    zuora_subscription_xf.lineage,
    zuora_subscription_xf.subscription_status,
    zuora_subscription_xf.exclude_from_renewal_report,
    
    zuora_rp.product_rate_plan_id,
    zuora_rp.rate_plan_name,
    zuora_rp.product_category,
    zuora_rp.delivery,
    zuora_rpc.rate_plan_charge_name,
    zuora_rpc.rate_plan_charge_number,
    zuora_rpc.mrr,
    zuora_rpc.tcv,
    DATE_TRUNC('month', zuora_subscription_xf.subscription_start_date::DATE)                     AS sub_start_month,
    DATE_TRUNC('month', dateadd('month', -1, zuora_subscription_xf.subscription_end_date::DATE)) AS sub_end_month,
    DATE_TRUNC('month', zuora_rpc.effective_start_date::DATE)                                    AS effective_start_month,
    DATE_TRUNC('month', dateadd('month', -1, zuora_rpc.effective_end_date::DATE))                AS effective_end_month,
    DATEDIFF(month, zuora_rpc.effective_start_date::DATE,
      zuora_rpc.effective_end_date::DATE)                                                        AS month_interval,
    zuora_rpc.effective_start_date,
    zuora_rpc.effective_end_date,
    zuora_subscription_xf.cohort_month,
    zuora_subscription_xf.cohort_quarter,
    zuora_rpc.unit_of_measure,
    zuora_rpc.quantity,
    FIRST_VALUE(orders.customer_id) 
      OVER (PARTITION BY orders.subscription_name_slugify 
            ORDER BY order_updated_at DESC)                                                      AS current_customer_id,
    FIRST_VALUE(gitlab_namespace_id) 
      OVER (PARTITION BY orders.subscription_name_slugify 
            ORDER BY gitlab_namespace_id IS NOT NULL DESC,
                      order_updated_at DESC)                                                     AS current_gitlab_namespace_id
            
  FROM orders 
  
  JOIN customers ON orders.customer_id = customers.customer_id
  JOIN zuora_subscription_xf
    ON orders.subscription_name_slugify = zuora_subscription_xf.subscription_name_slugify
  LEFT JOIN zuora_rp 
    ON zuora_rp.subscription_id = zuora_subscription_xf.subscription_id
      AND orders.product_rate_plan_id = zuora_rp.product_rate_plan_id
  LEFT JOIN zuora_rpc ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
  
  WHERE orders.product_rate_plan_id IS NOT NULL 
    AND orders.order_is_trial = FALSE
    AND orders.subscription_id IS NOT NULL

)

SELECT * 
FROM joined
