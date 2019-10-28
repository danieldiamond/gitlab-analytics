
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

, trials AS (
  
  SELECT *
  FROM orders_snapshots
  WHERE order_is_trial = TRUE
  
)

, converted_trials AS (
  
  SELECT DISTINCT
    trials.order_id,
    orders_snapshots.subscription_name_slugify
  FROM trials
  INNER JOIN orders_snapshots ON trials.order_id = orders_snapshots.order_id
  WHERE orders_snapshots.subscription_name_slugify IS NOT NULL
  
)
, joined AS (
  
  SELECT
    orders_snapshots.order_id, 
    orders_snapshots.gitlab_namespace_id,
    customers.customer_id,
    
      
    users.user_id                                           AS gitlab_user_id,
    IFF(users.user_id IS NOT NULL, TRUE, FALSE)             AS is_gitlab_user,
    users.user_created_at,
    
    namespaces.namespace_created_at,
    namespaces.namespace_type,
    
    IFF(converted_trials.order_id IS NOT NULL, TRUE, FALSE) AS is_converted,
    converted_trials.subscription_name_slugify,
    
    MIN(order_created_at)                                   AS order_created_at,
    MIN(orders_snapshots.order_start_date)::DATE            AS trial_start_date, 
    MAX(orders_snapshots.order_end_date)::DATE              AS trial_end_date
    
    
  FROM orders_snapshots
    INNER JOIN customers ON orders_snapshots.customer_id = customers.customer_id
    LEFT JOIN namespaces ON orders_snapshots.gitlab_namespace_id = namespaces.namespace_id
    LEFT JOIN users ON customers.customer_provider_user_id = users.user_id
    LEFT JOIN converted_trials ON orders_snapshots.order_id = converted_trials.order_id
  WHERE order_is_trial 
    AND order_start_date >= '2019-09-01'
  {{dbt_utils.group_by(10)}}
  
)

SELECT * 
FROM joined
