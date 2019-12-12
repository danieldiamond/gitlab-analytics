WITH top_level AS (
  SELECT *
  FROM analytics.gitlab_dotcom_namespaces_xf
  WHERE namespace_type = 'Group'
    AND parent_id IS NULL
),

sub_groups AS (
  SELECT *
  FROM analytics_staging.gitlab_dotcom_namespace_lineage
),

projects AS (
  SELECT *
  FROM analytics.gitlab_dotcom_projects_xf
),

all_children AS (
  SELECT
    top_level.namespace_id  AS ultimate_parent_id,
    'Namespace'             AS source_type,
    sub_groups.namespace_id AS source_id
  FROM top_level
    LEFT JOIN sub_groups
      ON top_level.namespace_id = sub_groups.ultimate_parent_id
//    UNION 
//    SELECT
//      top_level.namespace_id,
//      'Project',
//      projects.project_id
//    FROM top_level
//      LEFT JOIN projects
//        ON top_level.namespace_id = projects.namespace_ultimate_parent_id -- CHECK
),

members_max_access AS (
  SELECT
    all_children.ultimate_parent_id AS namespace_id,
    members.user_id,
    MAX(access_level) AS max_access_level
  FROM all_children
    INNER JOIN analytics.gitlab_dotcom_members AS members
      ON  all_children.source_id = members.source_id
      AND all_children.source_type = members.member_source_type
      AND is_currently_valid = True
  GROUP BY 1,2
),

members AS (
  SELECT
    namespace_id,
    COALESCE(COUNT(DISTINCT CASE WHEN max_access_level >= 20 THEN user_id END), 0) AS count_non_guest_members,
    COALESCE(COUNT(DISTINCT CASE WHEN max_access_level = 10 THEN user_id END), 0) AS count_guest_members
  FROM members_max_access
  GROUP BY 1
),

gl_subs AS (
  SELECT
    namespace_id,
    plan_id
  FROM analytics_staging.gitlab_dotcom_gitlab_subscriptions AS gl_subs
  WHERE True
    AND plan_id != 34
    AND is_trial = False
    AND is_currently_valid = True
),

customers AS (
  SELECT DISTINCT
    cust_customers.company AS company_name,
    cust_orders.customer_id AS customer_id,
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
    AND mrr>0
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
    customers.company_name,
    'https://customers.gitlab.com/admin/customer/' || customers.customer_id AS customers_portal_link,

    CASE
      WHEN summed_zuora.product_category = 'Gold' THEN count_non_guest_members
      ELSE count_non_guest_members + count_guest_members
    END AS count_seats_currently_used,
    summed_zuora.count_zuora_seats_entitled_to,
    count_seats_currently_used - count_zuora_seats_entitled_to AS count_seats_above_entitiled,
    summed_zuora.mrr_per_seat_on_current_subscription,
    count_seats_above_entitiled * mrr_per_seat_on_current_subscription AS "Seats Over x MRR per Seat",
    count_seats_above_entitiled * mrr_per_seat_on_current_subscription * 12 AS "12 x Seats Over x MRR per Seat",

    gl_subs.namespace_id,
    --gl_subs.plan_id,
    members.count_non_guest_members,
    members.count_guest_members,

    customers.zuora_subscription_id,
    'https://www.zuora.com/apps/Subscription.do?method=view&id=' || customers.zuora_subscription_id AS zuora_subscription_link,
    customers.zuora_account_id,
    'https://www.zuora.com/apps/CustomerAccount.do?method=view&id=' || customers.zuora_account_id AS zuora_account_link,
    customers.sfdc_account_id,
    'https://gitlab.my.salesforce.com/' || customers.sfdc_account_id AS sfdc_account_link,
    summed_zuora.subscription_start_date,
    DATEDIFF('days', subscription_start_date, CURRENT_DATE) AS subscription_days_old_on_12_11,
    (subscription_days_old_on_12_11 >= 365) AS subscription_over_one_year_on_12_11,
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
--SELECT * FROM members_max_access WHERE namespace_id = 3517177;

SELECT *
FROM final 
WHERE True
  AND count_seats_above_entitiled > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY count_zuora_seats_entitled_to DESC) = 1 --Handle multiple customers accounts tied to same namespace (rare)
ORDER BY count_seats_above_entitiled DESC

--SELECT SUM(mrr_per_seat_on_current_subscription * count_seats_above_entitiled) * 12 FROM final WHERE count_seats_above_entitiled > 0 AND subscription_days_old_on_12_06 > 365

--Consensys Systems UK Ltd.