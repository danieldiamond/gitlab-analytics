WITH customers AS (
  
    SELECT * 
    FROM {{ ref('customers_db_customers') }}
  
)

, orders AS (
  
    SELECT * 
    FROM {{ ref('customers_db_orders') }}
  
)

, trials AS (
  
    SELECT * 
    FROM {{ ref('customers_db_trials') }}
  
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
  
    SELECT *
    FROM orders
    WHERE orders.product_rate_plan_id IS NOT NULL 
      AND orders.order_is_trial = FALSE
      AND orders.subscription_id IS NOT NULL
)

, joined AS (
  
    SELECT DISTINCT
      zuora_rpc.rate_plan_charge_id,
      
      -- Foreign Keys
      orders_with_subscription.customer_id,
      orders_with_subscription.gitlab_namespace_id,
      orders_with_subscription.subscription_name_slugify,
      
      zuora_rp.product_rate_plan_id,
      zuora_rp.rate_plan_id,
      
      -- Subscription metadata
      zuora_subscription_xf.lineage,
      zuora_subscription_xf.oldest_subscription_in_cohort,
      zuora_subscription_xf.subscription_start_date,
      zuora_subscription_xf.subscription_end_date,
      zuora_subscription_xf.subscription_status,
      
      DATE_TRUNC('month', zuora_rpc.effective_start_date::DATE)          AS charge_effective_start_date,
      DATE_TRUNC('month', 
                    DATEADD('month', -1, zuora_rpc.effective_end_date::DATE)
                  )                                                      AS charge_effective_end_date,
      
      -- Product Category Info
      zuora_rp.delivery,
      zuora_rp.product_category,
      
      -- Financial Info
      IFF(zuora_rpc.created_by_id = '2c92a0fd55822b4d015593ac264767f2',
            TRUE, FALSE)                                                 AS is_purchased_through_subscription_portal,
      zuora_rpc.mrr,
      zuora_rpc.tcv,
      
      -- Orders metadata
      FIRST_VALUE(orders_with_subscription.customer_id) 
        OVER (PARTITION BY orders_with_subscription.subscription_name_slugify 
              ORDER BY orders_with_subscription.order_updated_at DESC)   AS current_customer_id,
      FIRST_VALUE(orders_with_subscription.gitlab_namespace_id) 
        OVER (PARTITION BY orders_with_subscription.subscription_name_slugify 
              ORDER BY orders_with_subscription.gitlab_namespace_id IS NOT NULL DESC,
                        orders_with_subscription.order_updated_at DESC)  AS current_gitlab_namespace_id,
      FIRST_VALUE(orders_with_subscription.customer_id) 
        OVER (PARTITION BY orders_with_subscription.subscription_name_slugify 
              ORDER BY orders_with_subscription.order_created_at ASC)    AS first_customer_id,
      
      -- Trial Info                  
      MAX(IFF(trials.order_id IS NOT NULL, TRUE, FALSE)) 
        OVER (PARTITION BY orders_with_subscription.subscription_name_slugify
              ORDER BY trial_start_date ASC)                             AS is_started_with_trial,
      FIRST_VALUE(trials.trial_start_date)
        OVER (PARTITION BY orders_with_subscription.subscription_name_slugify
              ORDER BY trial_start_date ASC)                             AS trial_start_date
    
    FROM orders_with_subscription 
    INNER JOIN customers ON orders_with_subscription.customer_id = customers.customer_id
    INNER JOIN zuora_subscription_xf
      ON orders_with_subscription.subscription_name_slugify = zuora_subscription_xf.subscription_name_slugify
    LEFT JOIN zuora_rp 
      ON zuora_rp.subscription_id = zuora_subscription_xf.subscription_id
      AND orders_with_subscription.product_rate_plan_id = zuora_rp.product_rate_plan_id
    INNER JOIN zuora_rpc 
      ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
      AND zuora_rpc.mrr > 0
      AND zuora_rpc.tcv > 0 
    LEFT JOIN trials ON orders_with_subscription.order_id = trials.order_id
    
    WHERE TRUE
      AND charge_effective_end_date >= charge_effective_start_date  

)

, joined_with_customer_and_namespace_list AS (
  
    SELECT DISTINCT
      rate_plan_charge_id,
      subscription_name_slugify,
      rate_plan_id,
      product_rate_plan_id,
      lineage,
      oldest_subscription_in_cohort,
      subscription_start_date,
      subscription_end_date,
      subscription_status,
      charge_effective_start_date,
      charge_effective_end_date,
      delivery,
      product_category,
      is_purchased_through_subscription_portal,
      mrr,
      tcv,
      current_customer_id,
      current_gitlab_namespace_id,
      first_customer_id,
      is_started_with_trial,
      trial_start_date,
      ARRAY_AGG(customer_id) 
        WITHIN GROUP (ORDER  BY customer_id ASC) AS customer_id_list,
      ARRAY_AGG(gitlab_namespace_id) 
        WITHIN GROUP (ORDER  BY customer_id ASC) AS gitlab_namespace_id_list
    FROM joined
    {{ dbt_utils.group_by(n=21) }}
    
)

SELECT * 
FROM joined_with_customer_and_namespace_list
