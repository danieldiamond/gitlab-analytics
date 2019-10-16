WITH customers AS (
  
  SELECT * 
  FROM {{ ref('customers_db_customers')}}

)

, orders AS (
  
  SELECT * 
  FROM {{ ref('customers_db_orders')}}

)

, joined AS (
  
  SELECT 
    customers.customer_id,
    IFF(orders.customer_id IS NOT NULL, TRUE, FALSE) AS is_converted,
    MIN(orders.order_created_at)                     AS first_order_created_at
    
  FROM customers
  LEFT JOIN orders ON customers.customer_id = orders.customer_id
  GROUP BY 1,2

)

SELECT * 
FROM joined
