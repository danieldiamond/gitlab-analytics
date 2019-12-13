WITH customers AS (
  
    SELECT * 
    FROM {{ ref('customers_db_customers')}}
  
)

, customers_db_latest_trials_per_namespace AS (
  
    SELECT * 
    FROM {{ ref('customers_db_latest_trials_per_namespace')}}
  
)

, gitlab_subscriptions AS (
  
    SELECT * 
    FROM {{ ref('gitlab_dotcom_gitlab_subscriptions_snapshots_base')}}
  
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

, namespace_with_latest_trial_date AS (
                                     
    SELECT 
      namespace_id, 
      MAX(gitlab_subscription_trial_ends_on)                      AS latest_trial_end_date,
      DATEADD('day', -30, MAX(gitlab_subscription_trial_ends_on)) AS estimated_latest_trial_start_date
    FROM gitlab_subscriptions
    WHERE gitlab_subscription_trial_ends_on IS NOT NULL
    GROUP BY 1

)

, trials_joined AS (

    SELECT
      namespace_with_latest_trial_date.namespace_id,
      namespace_with_latest_trial_date.latest_trial_end_date,
      COALESCE(customers_db_latest_trials_per_namespace.order_start_date, 
               namespace_with_latest_trial_date.estimated_latest_trial_start_date) AS latest_trial_start_date,
      customers.customer_id,
      customers.customer_provider_user_id,
      customers.country,
      customers.company_size
      
    FROM namespace_with_latest_trial_date
    LEFT JOIN customers_db_latest_trials_per_namespace 
      ON namespace_with_latest_trial_date.namespace_id = customers_db_latest_trials_per_namespace.gitlab_namespace_id
    LEFT JOIN customers 
      ON customers_db_latest_trials_per_namespace.customer_id = customers.customer_id

)

, converted_trials AS (
  
    SELECT DISTINCT
      trials_joined.namespace_id,
      orders_shapshots_excluding_ci_minutes.subscription_name_slugify,
      subscription.subscription_start_date
    FROM trials_joined
    INNER JOIN orders_shapshots_excluding_ci_minutes 
      ON trials_joined.namespace_id = orders_shapshots_excluding_ci_minutes.gitlab_namespace_id
    INNER JOIN zuora_subscription_with_positive_mrr_tcv AS subscription
      ON orders_shapshots_excluding_ci_minutes.subscription_name_slugify = subscription.subscription_name_slugify
      AND trials_joined.latest_trial_start_date <= subscription.subscription_start_date
    WHERE orders_shapshots_excluding_ci_minutes.subscription_name_slugify IS NOT NULL
  
)
, joined AS (
  
    SELECT
      trials_joined.namespace_id,
      trials_joined.customer_id,
      trials_joined.country,
      trials_joined.company_size,
        
      users.user_id                                           AS gitlab_user_id,
      IFF(users.user_id IS NOT NULL, TRUE, FALSE)             AS is_gitlab_user,
      users.created_at                                        AS user_created_at,
      
      
      namespaces.namespace_created_at,
      namespaces.namespace_type,
      
      trials_joined.latest_trial_start_date, 
      trials_joined.latest_trial_end_date,
      MIN(subscription_start_date)                            AS subscription_start_date  
    FROM trials_joined
    LEFT JOIN namespaces 
      ON trials_joined.namespace_id = namespaces.namespace_id
    LEFT JOIN users 
      ON trials_joined.customer_provider_user_id = users.user_id
    LEFT JOIN converted_trials  ON trials_joined.namespace_id = converted_trials.namespace_id
    {{dbt_utils.group_by(11)}}

)

SELECT *
FROM joined
