
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

, joined AS (
  
  SELECT
    orders_snapshots.order_id, 
    orders_snapshots.gitlab_namespace_id,
    customers.customer_id,
    
    customers.customer_provider, 
    customers.customer_provider_user_id,
    
    users.user_id                               AS gitlab_user_id,
    IFF(users.user_id IS NOT NULL, TRUE, FALSE) AS is_gitlab_user,
    users.user_created_at,
    
    namespaces.namespace_created_at,
    namespaces.namespace_type,
    
    MIN(orders_snapshots.order_start_date)      AS trial_start_date, 
    MAX(orders_snapshots.order_end_date)        AS trial_end_date
  FROM orders_snapshots
  JOIN customers ON orders_snapshots.customer_id = customers.customer_id
  LEFT JOIN namespaces ON orders_snapshots.gitlab_namespace_id = namespaces.namespace_id
  LEFT JOIN users ON customers.customer_provider_user_id = users.user_id
  WHERE order_is_trial 
    AND order_start_date >= '2019-09-01'
  {{dbt_utils.group_by(10)}}
  
)

SELECT * 
FROM joined
