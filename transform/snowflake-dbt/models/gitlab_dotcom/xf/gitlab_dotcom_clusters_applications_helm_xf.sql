WITH clusters AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_clusters') }}

),

clusters_applications_helm AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_clusters_applications_helm')}}

),

cluster_groups AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_cluster_groups')}}

),

cluster_projects AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_cluster_projects')}}

),

final AS (

    SELECT
      clusters_applications_helm.*,
      clusters.user_id,
      cluster_groups.cluster_group_id,
      cluster_projects.cluster_project_id
    FROM clusters_applications_helm
      INNER JOIN clusters
        ON clusters_applications_helm.cluster_id = clusters.cluster_id
      LEFT JOIN cluster_groups
        ON clusters.cluster_id = cluster_groups.cluster_id
      LEFT JOIN cluster_projects
        ON clusters.cluster_id = cluster_projects.cluster_id

)

SELECT *
FROM final
