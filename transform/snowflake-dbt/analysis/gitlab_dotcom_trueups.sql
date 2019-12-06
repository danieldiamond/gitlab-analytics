WITH members AS (
  SELECT
    source_id AS namespace_id,
    COALESCE(COUNT(DISTINCT CASE WHEN access_level > 20 THEN member_id END), 0) AS count_non_guest_members,
    COALESCE(COUNT(DISTINCT CASE WHEN access_level = 10 THEN member_id END), 0) AS count_guest_members
  FROM analytics.gitlab_dotcom_members
  WHERE member_source_type = 'Namespace'
  GROUP BY 1
),

gl_subs AS (
  SELECT *
  FROM analytics_staging.gitlab_dotcom_gitlab_subscriptions
  WHERE True
    AND plan_id != 34
    AND is_trial = False
),

customers AS (
  SELECT DISTINCT
    cust_orders.gitlab_namespace_id,
    cust_customers.zuora_account_id,
    cust_orders.subscription_id,
    sfdc_account_id
  FROM analytics_staging.customers_db_orders AS cust_orders
    LEFT JOIN analytics_staging.customers_db_customers AS cust_customers
      ON cust_orders.customer_id = cust_customers.customer_id
  WHERE 1=1
    --AND gitlab_namespace_id = 2707805
    AND (cust_orders.order_end_date IS NULL OR cust_orders.order_end_date > CURRENT_TIMESTAMP)
),

zuora AS (
  SELECT DISTINCT
    zuora_subscriptions.account_id,
    zuora_subscriptions.subscription_id,
    zuora_subscriptions.auto_renew,
    zuora_subscriptions.subscription_end_date::DATE AS subscription_end_date,
    zuora_rp.rate_plan_id,
    zuora_rpc.rate_plan_charge_id,
    zuora_rp.product_category,
    zuora_rpc.unit_of_measure,
    zuora_rpc.quantity
  FROM analytics_staging.zuora_subscription AS zuora_subscriptions
    INNER JOIN analytics_staging.zuora_rate_plan AS zuora_rp
      ON zuora_rp.subscription_id = zuora_subscriptions.subscription_id
    INNER JOIN analytics_staging.zuora_rate_plan_charge AS zuora_rpc
      ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
  WHERE subscription_status = 'Active'
  QUALIFY DENSE_RANK() OVER (PARTITION BY zuora_subscriptions.account_id ORDER BY segment DESC) = 1 -- TODO: if account join changes later on
),

summed_zuora AS (
  SELECT
    account_id, subscription_id, auto_renew, product_category, unit_of_measure, subscription_end_date,
    SUM(quantity) AS zuora_seat_quantity
  FROM zuora
  GROUP BY 1,2,3,4,5,6
)

SELECT 
  zuora_seat_quantity - count_non_guest_members AS trueup_amount,
  gl_subs.namespace_id,
  gl_subs.plan_id,
  members.count_non_guest_members,
  members.count_guest_members,
  customers.sfdc_account_id,
  customers.zuora_account_id,
  customers.subscription_id,
  summed_zuora.subscription_end_date,
  summed_zuora.unit_of_measure,
  summed_zuora.product_category,
  summed_zuora.zuora_seat_quantity,
  summed_zuora.auto_renew
FROM gl_subs
  INNER JOIN members
    ON gl_subs.namespace_id = members.namespace_id
  INNER JOIN customers
    ON gl_subs.namespace_id = customers.gitlab_namespace_id
  INNER JOIN summed_zuora --TODO: what if left join?
    ON customers.zuora_account_id = summed_zuora .account_id
    AND customers.subscription_id = summed_zuora .subscription_id
WHERE 1=1
ORDER BY trueup_amount
LIMIT 100


-- Users with NO JOIN? Floaters
-- CI Minutes? 2c92a0ff6e68f558016e7fec81811a47
-- 2c92a0ff6e68f558016e7fec81b11a4b? Real subscription?