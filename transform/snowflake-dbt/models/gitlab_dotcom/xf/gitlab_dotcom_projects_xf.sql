
{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

{% set paid_plans = (2, 3, 4) %}

WITH projects AS (

    SELECT * FROM {{ref('gitlab_dotcom_projects')}}

),

namespaces AS (

    SELECT * FROM {{ref('gitlab_dotcom_namespaces')}}

),

members AS (

    SELECT * FROM {{ref('gitlab_dotcom_members')}}

),

joined AS (
    SELECT
      projects.*,
      namespaces.namespace_name,
      namespaces.namespace_path,
      namespaces.plan_id                                           AS namespace_plan_id,
      COALESCE( (namespaces.plan_id IN {{ paid_plans }} ), False)  AS namespace_plan_is_paid,
      COALESCE(COUNT(DISTINCT members.member_id), 0)               AS member_count
    FROM projects
      LEFT JOIN members
        ON projects.project_id = members.source_id
        AND members.member_source_type = 'Project'
      LEFT JOIN namespaces
        ON projects.namespace_id = namespaces.namespace_id
    {{ dbt_utils.group_by(n=61) }} 
)

SELECT * 
FROM joined
