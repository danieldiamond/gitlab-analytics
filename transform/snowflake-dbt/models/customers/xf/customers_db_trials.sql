
WITH customers AS (
  
  SELECT * 
  FROM {{ ref('customers_db_customers')}}
  
)

, namespaces AS (
  
  SELECT * 
  FROM {{ ref('gitlab_dotcom_namespaces')}}
  
)
 
, orders_snapshots AS (
  
  SELECT * 
  FROM {{ ref('customers_db_orders_snapshots_base')}}
  
)

, users AS (
 
 SELECT * 
 FROM {{ ref('gitlab_dotcom_users')}}
 
)

, zuora_rate_plan AS (
 
 SELECT * 
 FROM {{ ref('zuora_rate_plan')}}
 
)

, zuora_base_mrr AS (
 
 SELECT * 
 FROM {{ ref('zuora_base_mrr')}}
 
)

, zuora_subscription_with_positive_mrr_tcv AS (
  
  SELECT DISTINCT
    subscription_name_slugify,
    subscription_start_date
  FROM zuora_base_mrr 
  
)

, ci_minutes_charges AS (
  
  SELECT *
  FROM zuora_rate_plan
  WHERE rate_plan_name = '1,000 CI Minutes'
  
)

, orders_shapshots_excluding_ci_minutes AS (
  
  SELECT orders_snapshots.*
  FROM orders_snapshots
  LEFT JOIN ci_minutes_charges 
    ON orders_snapshots.subscription_id = ci_minutes_charges.subscription_id
    AND orders_snapshots.product_rate_plan_id = ci_minutes_charges.product_rate_plan_id
  WHERE ci_minutes_charges.subscription_id IS NULL
  
)

, trials AS (
  
  SELECT *
  FROM orders_snapshots
  WHERE order_is_trial = TRUE
  
)

, converted_trials AS (
  
  SELECT DISTINCT
    trials.order_id,
    orders_shapshots_excluding_ci_minutes.subscription_name_slugify
  FROM trials
  INNER JOIN orders_shapshots_excluding_ci_minutes 
    ON trials.order_id = orders_shapshots_excluding_ci_minutes.order_id
  INNER JOIN zuora_subscription_with_positive_mrr_tcv AS subscription
    ON orders_shapshots_excluding_ci_minutes.subscription_name_slugify = subscription.subscription_name_slugify
      AND trials.order_start_date <= subscription.subscription_start_date
  WHERE orders_shapshots_excluding_ci_minutes.subscription_name_slugify IS NOT NULL
  
)
, joined AS (
  
  SELECT
    trials.order_id, 
    trials.gitlab_namespace_id,
    customers.customer_id,
    
      
    users.user_id                                           AS gitlab_user_id,
    IFF(users.user_id IS NOT NULL, TRUE, FALSE)             AS is_gitlab_user,
    users.user_created_at,
    
    
    namespaces.namespace_created_at,
    namespaces.namespace_type,
    
    IFF(converted_trials.order_id IS NOT NULL, TRUE, FALSE) AS is_converted,
    converted_trials.subscription_name_slugify,
    
    MIN(order_created_at)                                   AS order_created_at,
    MIN(trials.order_start_date)::DATE                      AS trial_start_date, 
    MAX(trials.order_end_date)::DATE                        AS trial_end_date
    
    
  FROM trials
    INNER JOIN customers ON trials.customer_id = customers.customer_id
    LEFT JOIN namespaces ON trials.gitlab_namespace_id = namespaces.namespace_id
    LEFT JOIN users ON customers.customer_provider_user_id = users.user_id
    LEFT JOIN converted_trials ON trials.order_id = converted_trials.order_id
  WHERE trials.order_start_date >= '2019-09-01'
  {{dbt_utils.group_by(10)}}
  
)

SELECT * 
FROM joined
