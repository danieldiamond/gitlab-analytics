-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}

{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

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

    SELECT merge_request_id,
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

), all_projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}

), gitlab_org_projects AS (

    SELECT project_id
    FROM all_projects
    WHERE namespace_id IN {{ get_internal_namespaces() }}

),  joined as (

    SELECT merge_requests.*,
           ARRAY_TO_STRING(agg_labels.agg_label,'|') AS masked_label_title,
           merge_request_metrics.merged_at,
           CASE WHEN merge_requests.target_project_id IN ({{is_project_included_in_engineering_metrics()}}) THEN TRUE
                ELSE FALSE
                END AS is_included_in_engineering_metrics,
           CASE
            WHEN gitlab_org_projects.project_id IS NOT NULL
             AND ARRAY_CONTAINS('community contribution'::variant, agg_labels.agg_label)
              THEN TRUE
            ELSE FALSE
           END as is_community_contributor_related,
           TIMESTAMPDIFF(HOURS, merge_requests.merge_request_created_at, merge_request_metrics.merged_at) as hours_to_merged_status

    FROM merge_requests
      LEFT JOIN agg_labels
        ON merge_requests.merge_request_id = agg_labels.merge_request_id
      LEFT JOIN merge_request_metrics
        ON merge_requests.merge_request_id = merge_request_metrics.merge_request_id
      LEFT JOIN gitlab_org_projects
        ON merge_requests.target_project_id = gitlab_org_projects.project_id
)

SELECT *
FROM joined
