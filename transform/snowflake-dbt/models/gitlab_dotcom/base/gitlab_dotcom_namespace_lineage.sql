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

), with_ultimate_parent AS (

  SELECT
    *,
    -- The last item of the lineage array is the ultimate parent
    GET(upstream_lineage, ARRAY_SIZE(upstream_lineage)-1) AS ultimate_parent_id
  FROM recursive_namespaces

), ultimate_parent_with_plan AS (

  SELECT
    with_ultimate_parent.*,
    plans.plan_id  AS ultimate_parent_plan_id,
    plans.title    AS ultimate_parent_plan_title
  FROM with_ultimate_parent
    LEFT JOIN gitlab_subscriptions
      ON with_ultimate_parent.ultimate_parent_id = gitlab_subscriptions.namespace_id
      --AND gitlab_subsriptions.is_currently_valid = True
    LEFT JOIN plans
      ON COALESCE(gitlab_subscriptions.plan_id, 34) = plans.plan_id

)

SELECT *
FROM ultimate_parent_with_plan
