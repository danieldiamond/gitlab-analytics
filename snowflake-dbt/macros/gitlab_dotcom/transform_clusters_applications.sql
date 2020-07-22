{%- macro transform_clusters_applications(base_model) -%}

  WITH base AS (

      SELECT *
      FROM {{ ref(base_model) }}

  ),

  clusters AS (

      SELECT *
      FROM {{ ref('gitlab_dotcom_clusters_xf') }}

  ),

  final AS (

      SELECT
        base.*,
        clusters.user_id,
        clusters.cluster_group_id,
        clusters.cluster_project_id,
        clusters.ultimate_parent_id
      FROM base
        INNER JOIN clusters
          ON base.cluster_id = clusters.cluster_id

  )

  SELECT *
  FROM final

{%- endmacro -%}
