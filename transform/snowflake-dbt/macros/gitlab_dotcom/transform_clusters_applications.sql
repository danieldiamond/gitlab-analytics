{%- macro transform_clusters_applications(base_model) -%}

  WITH base AS (

      SELECT *
      FROM {{ ref(base_model) }}

  ),

  clusters AS (

      SELECT *
      FROM {{ ref('gitlab_dotcom_clusters') }}

  ),

  cluster_groups AS (

      SELECT *
      FROM {{ ref('gitlab_dotcom_cluster_groups')}}

  ),

  cluster_projects AS (

      SELECT *
      FROM {{ ref('gitlab_dotcom_cluster_projects')}}

  ),

  groups AS (

      SELECT *
      FROM {{ ref('gitlab_dotcom_namespaces_xf')}}

  ),

  projects AS (

      SELECT *
      FROM {{ ref('gitlab_dotcom_projects_xf')}}

  ),

  final AS (

      SELECT
        base.*,
        clusters.user_id,
        cluster_groups.cluster_group_id,
        cluster_projects.cluster_project_id,
        COALESCE(groups.ultimate_namespace_id, projects.ultimate_namespace_id) AS ultimate_namespace_id
      FROM base
        INNER JOIN clusters
          ON base.cluster_id = clusters.cluster_id
        LEFT JOIN cluster_groups
          ON clusters.cluster_id = cluster_groups.cluster_id
        LEFT JOIN cluster_projects
          ON clusters.cluster_id = cluster_projects.cluster_id
        LEFT JOIN groups
          ON cluster_groups.cluster_group_id = groups.group_id
        LEFT JOIN projects
          ON cluster_projects.cluster_project_id = projects.project_id

  )

  SELECT *
  FROM final

{%- endmacro -%}
