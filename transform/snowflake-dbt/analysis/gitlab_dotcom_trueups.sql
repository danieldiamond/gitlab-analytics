WITH members_raw AS ( --Workaround until !1986 is merged
    SELECT
      id::INTEGER                                    AS member_id,
      access_level::INTEGER                          AS access_level,
      source_id::INTEGER                             AS source_id,
      source_type                                    AS member_source_type
    FROM raw.tap_postgres.gitlab_db_members
    WHERE _task_instance = (SELECT MAX(_task_instance) FROM raw.tap_postgres.gitlab_db_members)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
),

members AS (
  SELECT
    source_id AS namespace_id,
    COALESCE(COUNT(DISTINCT CASE WHEN access_level > 20 THEN member_id END), 0) AS count_non_guest_members,
    COALESCE(COUNT(DISTINCT CASE WHEN access_level = 10 THEN member_id END), 0) AS count_guest_members
  FROM members_raw --analytics.gitlab_dotcom_members AS members
  WHERE member_source_type = 'Namespace'
  GROUP BY 1
),

gitlab_subscriptions_raw AS ( --Workaround until !1986 is merged
    SELECT
      id::INTEGER                                   AS gitlab_subscription_id,
      start_date::DATE                              AS gitlab_subscription_start_date,
      end_date::DATE                                AS gitlab_subscription_end_date,
      trial_ends_on::DATE                           AS gitlab_subscription_trial_ends_on,
      namespace_id::INTEGER                         AS namespace_id,
      hosted_plan_id::INTEGER                       AS plan_id,
      max_seats_used::INTEGER                       AS max_seats_used,
      seats::INTEGER                                AS seats,
      trial::BOOLEAN                                AS is_trial,
      created_at::TIMESTAMP                         AS created_at,
      updated_at::TIMESTAMP                         AS updated_at
    FROM raw.tap_postgres.gitlab_db_gitlab_subscriptions
    WHERE _task_instance = (SELECT MAX(_task_instance) FROM raw.tap_postgres.gitlab_db_gitlab_subscriptions)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY _uploaded_at DESC) = 1
),

gl_subs AS (
  SELECT
    namespace_id,
    plan_id
  FROM gitlab_subscriptions_raw --analytics_staging.gitlab_dotcom_gitlab_subscriptions AS gl_subs
  WHERE True
    AND plan_id != 34
    AND is_trial = False
),

customers AS (
  SELECT DISTINCT
    cust_orders.gitlab_namespace_id,
    cust_customers.zuora_account_id,
    cust_orders.subscription_id AS zuora_subscription_id,
    sfdc_account_id
  FROM analytics_staging.customers_db_orders AS cust_orders
    LEFT JOIN analytics_staging.customers_db_customers AS cust_customers
      ON cust_orders.customer_id = cust_customers.customer_id
  WHERE 1=1
    AND (cust_orders.order_end_date IS NULL OR cust_orders.order_end_date > CURRENT_TIMESTAMP)
 
),

zuora AS (
  SELECT DISTINCT
    zuora_subscriptions.account_id,
    zuora_subscriptions.subscription_id,
    zuora_subscriptions.auto_renew,
    zuora_subscriptions.subscription_start_date::DATE AS subscription_start_date,
    zuora_subscriptions.subscription_end_date::DATE   AS subscription_end_date,
    zuora_accounts.created_date AS account_created_date,
    zuora_rp.rate_plan_id,
    zuora_rp.product_category,
    zuora_rpc.rate_plan_charge_id,
    zuora_rpc.mrr,
    zuora_rpc.unit_of_measure,
    zuora_rpc.quantity
  FROM analytics_staging.zuora_subscription AS zuora_subscriptions
    INNER JOIN analytics_staging.zuora_rate_plan AS zuora_rp
      ON zuora_rp.subscription_id = zuora_subscriptions.subscription_id
    INNER JOIN analytics_staging.zuora_rate_plan_charge AS zuora_rpc
      ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
    LEFT JOIN analytics_staging.zuora_account AS zuora_accounts
     ON zuora_subscriptions.account_id = zuora_accounts.account_id
  WHERE subscription_status = 'Active'
    AND zuora_rp.product_category IN ('Gold', 'Silver', 'Bronze')
    AND effective_end_date >= CURRENT_DATE
  QUALIFY DENSE_RANK() OVER (PARTITION BY zuora_subscriptions.account_id ORDER BY segment DESC) = 1 -- TODO: if account join changes later on
),

summed_zuora AS (
  SELECT
    account_id, account_created_date, subscription_id, auto_renew, product_category, unit_of_measure, subscription_start_date, subscription_end_date,
    SUM(quantity) AS count_zuora_seats_entitled_to,
    SUM(mrr) / SUM(quantity) AS mrr_per_seat_on_current_subscription
  FROM zuora
  GROUP BY 1,2,3,4,5,6,7,8
),

final AS (

  SELECT
    CASE
      WHEN summed_zuora.product_category = 'Gold' THEN count_non_guest_members
      ELSE count_non_guest_members + count_guest_members
    END AS count_seats_currently_used,
    summed_zuora.count_zuora_seats_entitled_to,
    count_seats_currently_used - count_zuora_seats_entitled_to AS count_seats_above_entitiled,
    summed_zuora.mrr_per_seat_on_current_subscription,
    count_seats_above_entitiled * mrr_per_seat_on_current_subscription AS "Seats Over x MRR per Seat",

    gl_subs.namespace_id,
    --gl_subs.plan_id,
    members.count_non_guest_members,
    members.count_guest_members,

    customers.zuora_subscription_id,
    'https://www.zuora.com/apps/Subscription.do?method=view&id' || customers.zuora_subscription_id AS zuora_subscription_link,
    customers.zuora_account_id,
    'https://www.zuora.com/apps/CustomerAccount.do?method=view&id=' || customers.zuora_account_id AS zuora_account_link,
    customers.sfdc_account_id,
    'https://gitlab.my.salesforce.com/' || customers.sfdc_account_id AS sfdc_account_link,
    summed_zuora.subscription_start_date,
    DATEDIFF('days', subscription_start_date, CURRENT_DATE) AS subscription_days_old_on_12_06,
    summed_zuora.subscription_end_date,
    summed_zuora.product_category,
    summed_zuora.auto_renew

    ,' ' AS metadata
    ,COUNT(*) OVER (PARTITION BY customers.zuora_account_id) AS count_per_zuora_account_id
    ,COUNT(*) OVER (PARTITION BY customers.zuora_subscription_id) AS count_per_zuora_subscription_id
    ,COUNT(*) OVER (PARTITION BY gl_subs.namespace_id) AS count_per_namespace_id
  FROM gl_subs
    INNER JOIN members
      ON gl_subs.namespace_id = members.namespace_id
    INNER JOIN customers
      ON gl_subs.namespace_id = customers.gitlab_namespace_id
    INNER JOIN summed_zuora
      ON customers.zuora_account_id = summed_zuora.account_id
      AND customers.zuora_subscription_id = summed_zuora.subscription_id
  WHERE 1=1
    /* Hard filter out exceptions */
    AND zuora_subscription_id NOT IN ('2c92a0ff6e68f558016e7fec81b11a4b')
  ORDER BY count_seats_above_entitiled DESC
)

SELECT *
FROM final 
WHERE True
  AND count_seats_above_entitiled > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY count_zuora_seats_entitled_to DESC) = 1 --Handle multiple customers accounts tied to same namespace (rare)
ORDER BY count_seats_above_entitiled DESC

--SELECT SUM(mrr_per_seat_on_current_subscription * count_seats_above_entitiled) * 12 FROM final WHERE count_seats_above_entitiled > 0