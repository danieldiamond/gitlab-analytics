-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- depends_on: {{ ref('projects_part_of_product') }}

WITH merge_requests AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_merge_requests')}}

), label_links AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_label_links')}}
    WHERE target_type = 'MergeRequest'

), all_labels AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_labels_xf')}}

), agg_labels AS (

    SELECT
      merge_request_id,
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY merge_request_id ASC) AS agg_label
    FROM merge_requests
    LEFT JOIN label_links
      ON merge_requests.merge_request_id = label_links.target_id
    LEFT JOIN all_labels
      ON label_links.label_id = all_labels.label_id
    GROUP BY merge_request_id

),  latest_merge_request_metric AS (

    SELECT MAX(merge_request_metric_id) AS target_id
    FROM {{ref('gitlab_dotcom_merge_request_metrics')}}
    GROUP BY merge_request_id

),  merge_request_metrics AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_merge_request_metrics')}}
    INNER JOIN latest_merge_request_metric
    ON merge_request_metric_id = target_id

), projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}

), namespace_lineage AS (

    SELECT
      namespace_id,
      ultimate_parent_id,
      ( ultimate_parent_id IN {{ get_internal_parent_namespaces() }} ) AS namespace_is_internal
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

), joined AS (

    SELECT
      merge_requests.*,
      projects.namespace_id,
      namespace_lineage.ultimate_parent_id,
      namespace_lineage.namespace_is_internal,
      ARRAY_TO_STRING(agg_labels.agg_label,'|') AS masked_label_title,
      merge_request_metrics.merged_at,
      IFF(merge_requests.target_project_id IN ({{is_project_included_in_engineering_metrics()}}),
        TRUE, FALSE)                               AS is_included_in_engineering_metrics,
      IFF(merge_requests.target_project_id IN ({{is_project_part_of_product()}}),
        TRUE, FALSE)                               AS is_part_of_product,
      CASE
      WHEN namespace_is_internal IS NOT NULL
        AND ARRAY_CONTAINS('community contribution'::variant, agg_labels.agg_label)
        THEN TRUE
      ELSE FALSE
      END AS is_community_contributor_related,
      TIMESTAMPDIFF(HOURS, merge_requests.merge_request_created_at, merge_request_metrics.merged_at) AS hours_to_merged_status

    FROM merge_requests
      LEFT JOIN agg_labels
        ON merge_requests.merge_request_id = agg_labels.merge_request_id
      LEFT JOIN merge_request_metrics
        ON merge_requests.merge_request_id = merge_request_metrics.merge_request_id
      LEFT JOIN projects
        ON merge_requests.target_project_id = projects.project_id
      LEFT JOIN namespace_lineage
        ON projects.namespace_id = namespace_lineage.namespace_id
)

SELECT *
FROM joined
