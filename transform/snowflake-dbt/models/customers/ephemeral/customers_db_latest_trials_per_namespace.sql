WITH orders_snapshots AS (
  
  SELECT * 
  FROM {{ ref('customers_db_orders_snapshots_base')}}
  
)

, trials_snapshots AS (
  
  SELECT *
  FROM orders_snapshots
  WHERE order_is_trial = TRUE
  
)

, latest_trials_from_trial_snapshot AS (
  
    SELECT *
    FROM trials_snapshots
    QUALIFY ROW_NUMBER() OVER (PARTITION BY gitlab_namespace_id ORDER BY valid_from DESC, order_id DESC) = 1

)  

SELECT * 
FROM latest_trials_from_trial_snapshot
