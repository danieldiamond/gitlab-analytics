with merge_requests as (

    SELECT * FROM {{ref('gitlab_dotcom_merge_requests')}}

), all_label_links as (

    SELECT * FROM {{ref('gitlab_dotcom_label_links')}}

), label_links as (

    SELECT * FROM all_label_links where target_type = 'MergeRequest'

), labels as (

    SELECT * FROM {{ref('gitlab_dotcom_labels_xf')}}

),  latest_merge_request_metric as (

    SELECT max(merge_request_metric_id) as target_id
    FROM {{ref('gitlab_dotcom_merge_request_metrics')}}
    GROUP BY merge_request_id

),  merge_request_metrics as (

    SELECT * FROM {{ref('gitlab_dotcom_merge_request_metrics')}} as a
    INNER JOIN latest_merge_request_metric as b
    on a.merge_request_metric_id = b.target_id

), all_projects as (

    SELECT * from {{ref('gitlab_dotcom_projects')}}

), gitlab_org_projects as (

    SELECT project_id FROM all_projects where namespace_id = 9970

),  joined as (

    SELECT
      merge_requests.*,
      lower(labels.masked_label_title) as label_title,
      merge_request_metrics.merged_at,

      CASE
        WHEN target_project_id IN (select * from gitlab_org_projects)
          AND lower(labels.masked_label_title) = 'community contribution'
          THEN TRUE
        ELSE FALSE
      END as is_community_contributor_related,
      TIMESTAMPDIFF(HOURS, merge_requests.merge_request_created_at, merge_request_metrics.merged_at) as hours_to_merged_status

    FROM
      merge_requests
      LEFT JOIN label_links on merge_requests.merge_request_id = label_links.target_id
      LEFT JOIN labels on label_links.label_id = labels.label_id
      LEFT JOIN merge_request_metrics on merge_requests.merge_request_id = merge_request_metrics.merge_request_id
)

SELECT * from joined