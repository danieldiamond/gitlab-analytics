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

/* For each top-level namespace, what are all of the projects and subgroups in the lineage? */
all_children AS (
  SELECT
    top_level.namespace_id  AS ultimate_parent_id,
    'Namespace'             AS source_type,
    sub_groups.namespace_id AS source_id
  FROM top_level
    LEFT JOIN sub_groups
      ON top_level.namespace_id = sub_groups.ultimate_parent_id
    UNION 
    SELECT
      top_level.namespace_id,
      'Project',
      projects.project_id
    FROM top_level
      LEFT JOIN projects
        ON top_level.namespace_id = projects.namespace_ultimate_parent_id -- CHECK
),

/* All direct members of subgroups within projects */
project_group_links AS (
  SELECT
    ultimate_parent_id,
    project_id,
    group_id,
    group_access AS group_access_level
  FROM all_children
    INNER JOIN analytics_staging.gitlab_dotcom_project_group_links AS project_group_links
      ON all_children.source_id = project_group_links.project_id
      AND all_children.source_type = 'Project'
      AND project_group_links.is_currently_valid = True
      AND COALESCE(expires_at, '9999-12-31') > CURRENT_DATE
      --AND group_access  > 10
),

members_unioned AS (
  SELECT
    user_id,
    source_id,
    members.member_source_type AS member_source_type,
    access_level
  FROM analytics.gitlab_dotcom_members AS members
  UNION ALL
  SELECT DISTINCT
    user_id ,
    project_id AS source_id,
    'Project' AS member_source_type,
    group_access_level AS access_level
  FROM project_group_links
    INNER JOIN analytics.gitlab_dotcom_members AS members
      ON project_group_links.group_id = members.source_id--members.ultimate_parent_id
      AND members.member_source_type = 'Namespace'
      AND is_currently_valid = True
      AND COALESCE(expires_at, '9999-12-31') > CURRENT_DATE
      --AND access_level > 10
),

members_max_access AS (
  SELECT
    all_children.ultimate_parent_id AS namespace_id,
    all_children.source_type,
    members.user_id,
    MAX(access_level) AS max_access_level
  FROM all_children
    INNER JOIN members_unioned AS members
      ON  all_children.source_id = members.source_id
      AND all_children.source_type = members.member_source_type
  GROUP BY 1,2,3
),

agg_by_user AS (
  SELECT
    namespace_id,
    user_id,
    MAX(IFF(source_type='Project', 1, 0))   AS is_member_of_one_project,
    MAX(IFF(source_type='Namespace', 1, 0)) AS is_member_of_one_group
  FROM members_max_access
  WHERE user_id IS NOT NULL
  GROUP BY 1,2
),

count_by_namespace AS (
  SELECT
    namespace_id,
    COUNT(*) AS count_users_not_in_groups,
    LISTAGG(user_id, ', ') AS users_not_in_groups
  FROM agg_by_user
  WHERE is_member_of_one_project = 1
    AND is_member_of_one_group = 0
  GROUP BY 1
),

gl_subs AS (
  SELECT
    namespace_id,
    plan_id,
    CASE
      WHEN plan_id = 2 THEN 4
      WHEN plan_id = 3 THEN 19 
      WHEN plan_id = 4 THEN 99
    END AS mrr_per_user
  FROM analytics_staging.gitlab_dotcom_gitlab_subscriptions AS gl_subs
  WHERE True
    AND plan_id != 34
    AND is_trial = False
    AND is_currently_valid = True
),

customers AS (
  SELECT DISTINCT
    cust_customers.company,
    cust_orders.gitlab_namespace_id
  FROM analytics_staging.customers_db_orders AS cust_orders
    LEFT JOIN analytics_staging.customers_db_customers AS cust_customers
      ON cust_orders.customer_id = cust_customers.customer_id
  WHERE 1=1
    AND (cust_orders.order_end_date IS NULL OR cust_orders.order_end_date > CURRENT_TIMESTAMP)
  AND gitlab_namespace_id NOT IN (
     4347861 -- GitLab internal
    ,5086770 -- V.P. 
  )
)

SELECT
  * --SUM(count_users_not_in_groups), SUM(count_users_not_in_groups * mrr_per_user)
FROM gl_subs
  INNER JOIN count_by_namespace
    ON gl_subs.namespace_id = count_by_namespace.namespace_id
  INNER JOIN customers
    ON gl_subs.namespace_id = customers.gitlab_namespace_id
 ORDER BY 5 DESC
;

