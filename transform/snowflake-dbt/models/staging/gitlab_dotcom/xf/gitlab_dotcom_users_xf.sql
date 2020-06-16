WITH customers AS (

    SELECT *
    FROM {{ ref('customers_db_customers') }}

)

, memberships AS (

    SELECT
      *,
      DECODE(membership_source_type,
          'individual_namespace', 0,
          'group_membership', 1,
          'project_membership', 2,
          'group_group_link', 3,
          'project_group_link', 4
      ) AS membership_source_type_order,
      IFF(namespace_id = ultimate_parent_id, TRUE, FALSE) AS is_ultimate_parent
    FROM {{ ref('gitlab_dotcom_memberships') }}
    WHERE ultimate_parent_plan_id != '34'

)

, plans AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_plans') }}

)

, trials AS  (

    SELECT *
    FROM {{ ref('customers_db_trials') }}

)

, users AS (

    SELECT
      {{ dbt_utils.star(from=ref('gitlab_dotcom_users'), except=["created_at", "first_name", "last_name", "notification_email", "public_email", "updated_at", "users_name"]) }},
      created_at AS user_created_at,
      updated_at AS user_updated_at
    FROM {{ ref('gitlab_dotcom_users') }}

)

, highest_paid_subscription_plan AS (

  SELECT DISTINCT

    user_id,

    COALESCE(
      MAX(plans.plan_is_paid) OVER (
        PARTITION BY user_id
      ),
    FALSE)  AS highest_paid_subscription_plan_is_paid,

    COALESCE(
      FIRST_VALUE(ultimate_parent_plan_id) OVER (
      PARTITION BY user_id
      ORDER BY
          (ultimate_parent_plan_id = 'trial'),
          ultimate_parent_plan_id DESC,
          membership_source_type_order,
          is_ultimate_parent DESC,
          membership_source_type
      , 34)
    ) AS highest_paid_subscription_plan_id,

    FIRST_VALUE(namespace_id) OVER (
      PARTITION BY user_id
      ORDER BY
        (ultimate_parent_plan_id = 'trial'),
        ultimate_parent_plan_id DESC,
        membership_source_type_order,
        is_ultimate_parent DESC,
        membership_source_type
    ) AS highest_paid_subscription_namespace_id,

    FIRST_VALUE(ultimate_parent_id) OVER (
      PARTITION BY user_id
      ORDER BY
        (ultimate_parent_plan_id = 'trial'),
        ultimate_parent_plan_id DESC,
        membership_source_type_order,
        is_ultimate_parent DESC,
        membership_source_type
    ) AS highest_paid_subscription_ultimate_parent_id,

    FIRST_VALUE(membership_source_type) OVER (
      PARTITION BY user_id
      ORDER BY
        (ultimate_parent_plan_id = 'trial'),
        ultimate_parent_plan_id DESC,
        membership_source_type_order,
        is_ultimate_parent DESC,
        membership_source_type
    )  AS highest_paid_subscription_inheritance_source_type,

    FIRST_VALUE(membership_source_id) OVER (
      PARTITION BY user_id
      ORDER BY
        (ultimate_parent_plan_id = 'trial'),
        ultimate_parent_plan_id DESC,
        membership_source_type_order,
        is_ultimate_parent DESC,
        membership_source_type
    )  AS highest_paid_subscription_inheritance_source_id

  FROM memberships
    LEFT JOIN plans
      ON memberships.ultimate_parent_plan_id = plans.plan_id::VARCHAR

)

, customers_with_trial AS (

  SELECT
    customers.customer_provider_user_id                         AS user_id,
    MIN(customers.customer_id)                                  AS first_customer_id,
    MIN(customers.customer_created_at)                          AS first_customer_created_at,
    ARRAY_AGG(customers.customer_id)
        WITHIN GROUP (ORDER  BY customers.customer_id)          AS customer_id_list,
    MAX(IFF(order_id IS NOT NULL, TRUE, FALSE))                 AS has_started_trial,
    MIN(trial_start_date)                                       AS has_started_trial_at
  FROM customers
    LEFT JOIN trials
      ON customers.customer_id = trials.customer_id
  WHERE customers.customer_provider = 'gitlab'
  GROUP BY 1

)

, joined AS (
  SELECT
    users.*,
    TIMESTAMPDIFF(DAYS, user_created_at, last_activity_on)                       AS days_active,
    TIMESTAMPDIFF(DAYS, user_created_at, CURRENT_TIMESTAMP(2))                   AS account_age,
    CASE
      WHEN account_age <= 1 THEN '1 - 1 day or less'
      WHEN account_age <= 7 THEN '2 - 2 to 7 days'
      WHEN account_age <= 14 THEN '3 - 8 to 14 days'
      WHEN account_age <= 30 THEN '4 - 15 to 30 days'
      WHEN account_age <= 60 THEN '5 - 31 to 60 days'
      WHEN account_age > 60 THEN '6 - Over 60 days'
    END                                                                           AS account_age_cohort,

    highest_paid_subscription_plan.highest_paid_subscription_plan_id,
    highest_paid_subscription_plan.highest_paid_subscription_plan_is_paid         AS is_paid_user,
    highest_paid_subscription_plan.highest_paid_subscription_namespace_id,
    highest_paid_subscription_plan.highest_paid_subscription_ultimate_parent_id,
    highest_paid_subscription_plan.highest_paid_subscription_inheritance_source_type,
    highest_paid_subscription_plan.highest_paid_subscription_inheritance_source_id,

    IFF(customers_with_trial.first_customer_id IS NOT NULL, TRUE, FALSE)          AS has_customer_account,
    customers_with_trial.first_customer_created_at,
    customers_with_trial.first_customer_id,
    customers_with_trial.customer_id_list,
    customers_with_trial.has_started_trial,
    customers_with_trial.has_started_trial_at

  FROM users
    LEFT JOIN highest_paid_subscription_plan
      ON users.user_id = highest_paid_subscription_plan.user_id
    LEFT JOIN customers_with_trial
      ON users.user_id::VARCHAR = customers_with_trial.user_id::VARCHAR

)

SELECT *
FROM joined
