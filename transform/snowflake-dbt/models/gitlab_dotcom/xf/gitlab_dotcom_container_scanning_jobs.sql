{{
  config({
    "materialized": "incremental",
    "unique_key": "ci_build_id"
  })
}}

WITH ci_builds AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_builds') }}
    {% if is_incremental() %}

      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), projects AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_projects') }}

), namespace_lineage AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

), gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base')}}

), secure_ci_builds AS (
  
    SELECT 
      *,
      'container_scanning' AS secure_ci_job_type
    FROM ci_builds 
    WHERE ci_build_name ILIKE '%container_scanning%'
)

, joined AS (
  
    SELECT 
      secure_ci_builds.*,
      namespace_lineage.namespace_is_internal      AS is_internal_job,
      namespace_lineage.ultimate_parent_id,
      namespace_lineage.ultimate_parent_plan_id,
      namespace_lineage.ultimate_parent_plan_title,
      namespace_lineage.ultimate_parent_plan_is_paid,

      CASE
        WHEN gitlab_subscriptions.is_trial
          THEN 'trial'
        ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
      END AS plan_id_at_job_creation
    FROM secure_ci_builds
    LEFT JOIN projects
      ON secure_ci_builds.ci_build_project_id = projects.project_id
    LEFT JOIN namespace_lineage
      ON projects.namespace_id = namespace_lineage.namespace_id
    LEFT JOIN gitlab_subscriptions
      ON namespace_lineage.ultimate_parent_id = gitlab_subscriptions.namespace_id
      AND secure_ci_builds.created_at BETWEEN gitlab_subscriptions.valid_from AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
  
)

SELECT *
FROM secure_ci_builds
