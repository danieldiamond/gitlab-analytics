{{ config({
    "schema": "staging"
    })
}}

WITH RECURSIVE namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces')}}

), gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}
    WHERE is_currently_valid = TRUE

), plans AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_plans')}}

), recursive_namespaces(namespace_id, parent_id, upstream_lineage) AS (

  -- Select all namespaces without parents
  SELECT
    namespace_id,
    namespaces.parent_id,
    TO_ARRAY(namespace_id)                                      AS upstream_lineage -- Initiate lineage array
  FROM namespaces
  WHERE namespaces.parent_id IS NULL

  UNION ALL

  -- Recursively iterate through each of the children namespaces
  SELECT
    iter.namespace_id,
    iter.parent_id,
    ARRAY_INSERT(anchor.upstream_lineage, 0, iter.namespace_id)  AS upstream_lineage -- Copy the lineage array of parent, inserting self at start
  FROM recursive_namespaces AS anchor -- Parent namespace
    INNER JOIN namespaces  AS iter -- Child namespace
      ON anchor.namespace_id = iter.parent_id

), extracted AS (

  SELECT
    *,
    -- The last item of the lineage array is the ultimate parent
    GET(upstream_lineage, ARRAY_SIZE(upstream_lineage)-1) AS ultimate_parent_id
  FROM recursive_namespaces

  /* Select children with deleted parents. These are missed by the top-down recursive CTE.
     This is quite rare (n=79 on 2020-01-06). */
  UNION SELECT
    namespaces.namespace_id, 
    namespaces.parent_id,
    NULL AS upstream_lineage,
    NULL AS ultimate_parent_id
  FROM namespaces
  WHERE parent_id NOT IN (SELECT DISTINCT namespace_id FROM namespaces)

), with_plans AS (

  SELECT

    extracted.*,
    (ultimate_parent_id IN {{ get_internal_parent_namespaces() }})   AS namespace_is_internal,
    namespace_plans.plan_id                                          AS namespace_plan_id,
    namespace_plans.title                                            AS namespace_plan_title,
    namespace_plans.plan_is_paid                                     AS namespace_plan_is_paid,
    ultimate_parent_plans.plan_id                                    AS ultimate_parent_plan_id,
    ultimate_parent_plans.title                                      AS ultimate_parent_plan_title,
    ultimate_parent_plans.plan_is_paid                               AS ultimate_parent_plan_is_paid
  FROM extracted
    -- Get plan information for the namespace.
    LEFT JOIN gitlab_subscriptions AS namespace_gitlab_subscriptions
      ON extracted.namespace_id = namespace_gitlab_subscriptions.namespace_id
    LEFT JOIN plans AS namespace_plans
      ON COALESCE(namespace_gitlab_subscriptions.plan_id, 34) = namespace_plans.plan_id
    -- Get plan information for the ultimate parent namespace.
    LEFT JOIN gitlab_subscriptions AS ultimate_parent_gitlab_subscriptions
      ON extracted.ultimate_parent_id = ultimate_parent_gitlab_subscriptions.namespace_id
    LEFT JOIN plans AS ultimate_parent_plans
      ON COALESCE(ultimate_parent_gitlab_subscriptions.plan_id, 34) = ultimate_parent_plans.plan_id

)

SELECT *
FROM with_plans
