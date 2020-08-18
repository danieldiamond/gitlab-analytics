WITH date_details AS (

  SELECT
    date_day,
    first_day_of_month
  FROM{{ ref("date_details") }}
  WHERE last_day_of_month = date_day
    AND date_day > '2020-03-01'
    --AND date_day < CURRENT_DATE

), project_statistics_snapshot AS (

  SELECT
    *,
    IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
  FROM {{ ref("gitlab_dotcom_project_statistics_snapshots_base") }}

), namespaces_child AS (

  SELECT *
  FROM {{ ref("gitlab_dotcom_namespaces_xf") }}

), namespaces_parent AS (

  SELECT *
  FROM {{ ref("gitlab_dotcom_namespaces_xf") }}

), namespace_subscription_snapshot AS (

  SELECT
  *,
  IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
  FROM {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base") }}

), joined AS (

  SELECT
    date_details.first_day_of_month                                                                             AS month,
    project_statistics_snapshot.project_id                                                                      AS project_id,
    project_statistics_snapshot.namespace_id                                                                    AS namespace_id,
    COALESCE((namespace_subscription_snapshot.namespace_id IN {{ get_internal_parent_namespaces() }}), FALSE)   AS namespace_is_internal,
    namespace_subscription_snapshot.plan_id                                                                     AS plan_id,
    MAX(project_statistics_snapshot.storage_size/POW(1024,3))                                                   AS storage_size_GB,
    MAX(project_statistics_snapshot.repository_size/POW(1024,3))                                                AS repository_size_GB,
    MAX(project_statistics_snapshot.lfs_objects_size/POW(1024,3))                                               AS lfs_objects_size_GB,
    MAX(project_statistics_snapshot.build_artifacts_size/POW(1024,3))                                           AS build_artifacts_size_GB,
    MAX(project_statistics_snapshot.packages_size/POW(1024,3))                                                  AS packages_size_GB,
    MAX(project_statistics_snapshot.wiki_size/POW(1024,3))                                                      AS wiki_size_GB
    FROM date_details
    INNER JOIN project_statistics_snapshot
      ON date_details.date_day BETWEEN project_statistics_snapshot.valid_from AND project_statistics_snapshot.valid_to_
    INNER JOIN namespaces_child
      ON project_statistics_snapshot.namespace_id = namespaces_child.namespace_id
    INNER JOIN namespaces_parent
      ON namespaces_child.namespace_ultimate_parent_id = namespaces_parent.namespace_id
    INNER JOIN namespace_subscription_snapshot
      ON namespaces_parent.namespace_id = namespace_subscription_snapshot.namespace_id
        AND date_details.date_day BETWEEN namespace_subscription_snapshot.valid_from AND namespace_subscription_snapshot.valid_to_
    GROUP BY 1,2,3,4,5
)

SELECT * FROM joined
