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
    
    zuora_rp.rate_plan_id,
    zuora_rp.product_rate_plan_id,
    zuora_rpc.rate_plan_charge_id,
    
    zuora_subscription_xf.lineage,
    zuora_subscription_xf.oldest_subscription_in_cohort,
    zuora_subscription_xf.subscription_start_date,
    zuora_subscription_xf.subscription_status,
    
    IFF(zuora_rpc.created_by_id = '2c92a0fd55822b4d015593ac264767f2',
          TRUE, FALSE)                       AS is_purchased_through_subscription_portal,
    FIRST_VALUE(orders.customer_id) 
      OVER (PARTITION BY orders.subscription_name_slugify 
            ORDER BY order_updated_at DESC)  AS current_customer_id,
    FIRST_VALUE(orders.customer_id) 
      OVER (PARTITION BY orders.subscription_name_slugify 
            ORDER BY order_created_at ASC)   AS first_customer_id,
    FIRST_VALUE(gitlab_namespace_id) 
      OVER (PARTITION BY orders.subscription_name_slugify 
            ORDER BY gitlab_namespace_id IS NOT NULL DESC,
                      order_updated_at DESC) AS current_gitlab_namespace_id
            
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
