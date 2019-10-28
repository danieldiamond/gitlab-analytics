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

, orders_with_subscription AS (
  
    SELECT
      *
    FROM orders
    WHERE orders.product_rate_plan_id IS NOT NULL 
      AND orders.order_is_trial = FALSE
      AND orders.subscription_id IS NOT NULL
)

, joined AS (
  
    SELECT DISTINCT
      orders.subscription_name_slugify,
      orders.customer_id,
      orders.gitlab_namespace_id,
      
      zuora_rp.rate_plan_id,
      zuora_rp.product_rate_plan_id,
      zuora_rpc.rate_plan_charge_id,
      
      zuora_subscription_xf.lineage,
      zuora_subscription_xf.oldest_subscription_in_cohort,
      zuora_subscription_xf.subscription_start_date,
      zuora_subscription_xf.subscription_status,
      
      date_trunc('month', zuora_rpc.effective_start_date :: DATE) AS effective_start_date,
      date_trunc('month', 
                    dateadd('month', -1, zuora_rpc.effective_end_date :: DATE)
                  )                                               AS effective_end_date,
      
      
      IFF(zuora_rpc.created_by_id = '2c92a0fd55822b4d015593ac264767f2',
            TRUE, FALSE)                                          AS is_purchased_through_subscription_portal,
      FIRST_VALUE(orders.customer_id) 
        OVER (PARTITION BY orders.subscription_name_slugify 
              ORDER BY order_updated_at DESC)                     AS current_customer_id,
      FIRST_VALUE(orders.customer_id) 
        OVER (PARTITION BY orders.subscription_name_slugify 
              ORDER BY order_created_at ASC)                      AS first_customer_id,
      FIRST_VALUE(orders.gitlab_namespace_id) 
        OVER (PARTITION BY orders.subscription_name_slugify 
              ORDER BY gitlab_namespace_id IS NOT NULL DESC,
                        order_updated_at DESC)                    AS current_gitlab_namespace_id
              
    FROM orders 
    INNER JOIN customers ON orders.customer_id = customers.customer_id
    INNER JOIN zuora_subscription_xf
      ON orders.subscription_name_slugify = zuora_subscription_xf.subscription_name_slugify
    LEFT JOIN zuora_rp 
      ON zuora_rp.subscription_id = zuora_subscription_xf.subscription_id
      AND orders.product_rate_plan_id = zuora_rp.product_rate_plan_id
    LEFT JOIN zuora_rpc ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
    
    WHERE orders.product_rate_plan_id IS NOT NULL 
      AND orders.order_is_trial = FALSE
      AND orders.subscription_id IS NOT NULL
      AND zuora_rpc.mrr > 0
      AND zuora_rpc.tcv > 0
      AND effective_end_date >= effective_start_date  

)

, joined_with_customer_and_namespace_list AS (
  
    SELECT DISTINCT
      subscription_name_slugify,
      rate_plan_id,
      product_rate_plan_id,
      rate_plan_charge_id,
      lineage,
      oldest_subscription_in_cohort,
      subscription_start_date,
      subscription_status,
      is_purchased_through_subscription_portal,
      current_customer_id,
      first_customer_id,
      current_gitlab_namespace_id,
      ARRAY_AGG(customer_id) 
        WITHIN GROUP (ORDER  BY 1) AS customer_id_list,
      ARRAY_AGG(gitlab_namespace_id) 
        WITHIN GROUP (ORDER  BY 1) AS gitlab_namespace_id_list
    FROM joined
    {{ dbt_utils.group_by(n=12) }}
    
)

SELECT * 
FROM joined_with_customer_and_namespace_list
