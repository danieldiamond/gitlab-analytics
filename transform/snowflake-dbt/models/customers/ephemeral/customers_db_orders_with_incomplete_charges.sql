{{ config({
    "schema": "temporary"
    })
}}

WITH customers AS (
  
    SELECT * 
    FROM {{ ref('customers_db_customers') }}
  
)

, orders_snapshots AS (
  
    SELECT * 
    FROM {{ ref('customers_db_orders_snapshots_base') }}
  
)

, orders_with_valid_charges AS (
  
    SELECT * 
    FROM {{ ref('customers_db_orders_with_valid_charges') }}
  
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

, orders_with_subscriptions_without_product_plan_rate AS (
  
  SELECT DISTINCT
    order_id,
    subscription_id,
    subscription_name_slugify,
    customer_id,
    gitlab_namespace_id,
    product_rate_plan_id,
    FIRST_VALUE(order_created_at) 
      OVER (PARTITION BY order_id
            ORDER BY valid_from ASC) AS order_created_at,
    FIRST_VALUE(order_updated_at) 
      OVER (PARTITION BY order_id
            ORDER BY valid_to ASC)   AS order_updated_at
  FROM orders_snapshots
  WHERE orders_snapshots.product_rate_plan_id IS NULL 
    AND orders_snapshots.order_is_trial = FALSE
    AND orders_snapshots.subscription_id IS NOT NULL

)

, joined AS (
  
    SELECT DISTINCT
      zuora_rpc.rate_plan_charge_id,
      
      -- Foreign Keys
      opr.customer_id,
      opr.gitlab_namespace_id,
      opr.subscription_name_slugify,
      zuora_rp.rate_plan_id,
      
      -- Financial Info
      IFF(zuora_rpc.created_by_id = '2c92a0fd55822b4d015593ac264767f2',
            TRUE, FALSE)                            AS is_purchased_through_subscription_portal,
      
      -- Orders metadata
      FIRST_VALUE(opr.customer_id) 
        OVER (PARTITION BY opr.subscription_name_slugify 
              ORDER BY opr.order_updated_at DESC)   AS current_customer_id,
      FIRST_VALUE(opr.gitlab_namespace_id) 
        OVER (PARTITION BY opr.subscription_name_slugify 
              ORDER BY opr.gitlab_namespace_id IS NOT NULL DESC,
                        opr.order_updated_at DESC)  AS current_gitlab_namespace_id,
      FIRST_VALUE(opr.customer_id) 
        OVER (PARTITION BY opr.subscription_name_slugify 
              ORDER BY opr.order_created_at ASC)    AS first_customer_id,
      
      -- Trial Info                  
      MAX(IFF(trials.order_id IS NOT NULL, TRUE, FALSE)) 
        OVER (PARTITION BY opr.subscription_name_slugify
              ORDER BY trial_start_date ASC)        AS is_started_with_trial,
      FIRST_VALUE(trials.trial_start_date)
        OVER (PARTITION BY opr.subscription_name_slugify
              ORDER BY trial_start_date ASC)        AS trial_start_date
    
    FROM orders_with_subscriptions_without_product_plan_rate AS opr 
    INNER JOIN customers ON opr.customer_id = customers.customer_id
    INNER JOIN zuora_subscription_xf
      ON opr.subscription_name_slugify = zuora_subscription_xf.subscription_name_slugify
    LEFT JOIN zuora_rp 
      ON zuora_rp.subscription_id = zuora_subscription_xf.subscription_id
      AND opr.product_rate_plan_id = zuora_rp.product_rate_plan_id
    INNER JOIN zuora_rpc 
      ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
    LEFT JOIN trials ON opr.order_id = trials.order_id

)

, joined_with_customer_and_namespace_list AS (
  
    SELECT DISTINCT
      rate_plan_charge_id,
      subscription_name_slugify,
      rate_plan_id,
      is_purchased_through_subscription_portal,
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
    {{ dbt_utils.group_by(n=9) }}
    
)

, filtered_out_charges_with_valid_data AS (
  
  SELECT joined_with_customer_and_namespace_list.* 
  FROM joined_with_customer_and_namespace_list
  LEFT JOIN orders_with_valid_charges
    ON joined_with_customer_and_namespace_list.rate_plan_charge_id = orders_with_valid_charges.rate_plan_charge_id
  WHERE orders_with_valid_charges.rate_plan_charge_id IS NULL
)

SELECT * 
FROM filtered_out_charges_with_valid_data
