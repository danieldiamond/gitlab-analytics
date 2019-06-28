{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

{% set paid_plans = (2, 3, 4) %}


WITH namespaces AS (

    SELECT * FROM {{ref('gitlab_dotcom_namespaces')}}

),

members AS (

    SELECT * FROM {{ref('gitlab_dotcom_members')}}

),

projects AS (

    SELECT * FROM {{ref('gitlab_dotcom_projects')}}

),

joined AS (
    SELECT
      namespaces.*,
      COALESCE( (namespaces.plan_id IN {{ paid_plans }} ), False)  AS namespace_plan_is_paid,
      COALESCE(COUNT(DISTINCT members.member_id), 0)               AS member_count,
      COALESCE(COUNT(DISTINCT projects.project_id), 0)             AS project_count
    FROM namespaces
      LEFT JOIN members
        ON namespaces.namespace_id = members.source_id
        AND members.member_source_type = 'Namespace'
      LEFT JOIN projects
        ON namespaces.namespace_id = projects.namespace_id
    {{ dbt_utils.group_by(n=26) }} 
)

SELECT *
FROM joined
