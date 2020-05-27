WITH resource_label_events AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_label_events') }}
  
)

, epics AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_epics') }} 

)

, issues AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_xf') }} 

)

, mrs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_requests_xf') }} 

)

, joined AS (

    SELECT 
      resource_label_events.*,
      COALESCE(epics.group_id,
                issues.namespace_id,
                mrs.namespace_id) AS namespace_id
    FROM resource_label_events
    LEFT JOIN epics
      ON resource_label_events.epic_id = epics.epic_id
    LEFT JOIN issues
      ON resource_label_events.issue_id = issues.issue_id
    LEFT JOIN mrs
      ON resource_label_events.merge_request_id = mrs.merge_request_id

)

SELECT *
FROM joined
