WITH customers AS (
  
  SELECT *
  FROM {{ ref('customers_db_customers') }}
  
)

, groups AS  (

  SELECT *
  FROM {{ ref('gitlab_dotcom_groups_xf') }}

)

, members AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_members') }}
  WHERE is_currently_valid = TRUE

)

, namespaces AS  (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

)

, projects AS  (

  SELECT *
  FROM {{ ref('gitlab_dotcom_projects_xf') }}

)

, trials AS  (

  SELECT *
  FROM {{ ref('customers_db_trials') }}

)

, users AS (

  SELECT 
  {{ dbt_utils.star(from=ref('gitlab_dotcom_users'), except=["created_at", "updated_at"]) }},
    created_at AS user_created_at,
    updated_at AS user_updated_at
  FROM {{ ref('gitlab_dotcom_users') }}

)

, user_namespace_subscriptions AS (

  SELECT
    owner_id                AS user_id,
    namespace_id,
    plan_id,
    '0. personal_namespace' AS inheritance_source
  FROM namespaces
  WHERE namespace_type = 'Individual'
    AND namespace_plan_is_paid
)

, group_members AS (
  -- always inherits

  SELECT
    members.user_id,
    groups.group_id,
    groups.plan_id,
    groups.visibility_level,
    groups.plan_id        AS inherited_subscription_plan_id,
    '1. group'            AS inheritance_source

  FROM members
  INNER JOIN groups
    ON members.source_id = groups.group_id
      AND groups.group_plan_is_paid
  WHERE member_type = 'GroupMember'
    AND (members.expires_at >= CURRENT_DATE OR members.expires_at IS NULL)
)

, project_members AS (
  -- if project belongs to group apply same rules as above
  -- if project belongs to personal namespace. never apply any subscriptions

    SELECT
      members.user_id,
      projects.project_id,
      projects.visibility_level AS project_visibility_level,
      groups.plan_id,
      groups.visibility_level   AS namespace_visibility_level,
      groups.group_id,
      groups.plan_id            AS inherited_subscription_plan_id,
      '2. project'              AS inheritance_source

    FROM members
    LEFT JOIN projects
      ON members.source_id = projects.project_id
    INNER JOIN groups
      ON projects.namespace_id = groups.group_id
        AND groups.group_plan_is_paid
    WHERE members.member_type = 'ProjectMember'
      AND (members.expires_at >= CURRENT_DATE OR members.expires_at IS NULL)

)

, user_paid_subscription_plan_lk AS (

  (

    SELECT
      user_id,
      group_id AS namespace_id,
      NULL     AS project_id,
      inherited_subscription_plan_id,
      inheritance_source

    FROM group_members

  )

  UNION

  (

    SELECT
      user_id,
      group_id AS namespace_id,
      project_id,
      inherited_subscription_plan_id,
      inheritance_source

    FROM project_members

  )

  UNION

  (

    SELECT
      user_id,
      namespace_id,
      NULL AS project_id,
      plan_id,
      inheritance_source

    FROM user_namespace_subscriptions

  )

)

, highest_paid_subscription_plan AS (

  SELECT
    DISTINCT
    user_id,
    MAX(inherited_subscription_plan_id) OVER
      (PARTITION BY user_id)         AS highest_paid_subscription_plan_id,
    FIRST_VALUE(inheritance_source) OVER
      (PARTITION BY user_id
        ORDER BY inherited_subscription_plan_id DESC,
                  inheritance_source ASC,
                  namespace_id ASC)  AS highest_paid_subscription_inheritance_source,
    FIRST_VALUE(namespace_id) OVER
      (PARTITION BY user_id
        ORDER BY inherited_subscription_plan_id DESC,
                  inheritance_source ASC,
                  namespace_id ASC) AS highest_paid_subscription_namespace_id,
    FIRST_VALUE(project_id) OVER
      (PARTITION BY user_id
        ORDER BY inherited_subscription_plan_id DESC,
                  inheritance_source ASC,
                  namespace_id ASC) AS highest_paid_subscription_project_id
  FROM user_paid_subscription_plan_lk

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
  LEFT JOIN trials ON customers.customer_id = trials.customer_id
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
    END                                                                          AS account_age_cohort,
    
    highest_paid_subscription_plan.highest_paid_subscription_plan_id,
    highest_paid_subscription_plan.highest_paid_subscription_plan_id IS NOT NULL AS is_paid_user,
    highest_paid_subscription_plan.highest_paid_subscription_inheritance_source,
    highest_paid_subscription_plan.highest_paid_subscription_namespace_id,
    highest_paid_subscription_plan.highest_paid_subscription_project_id,
    
    IFF(customers_with_trial.first_customer_id IS NOT NULL, TRUE, FALSE)         AS has_customer_account,
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
