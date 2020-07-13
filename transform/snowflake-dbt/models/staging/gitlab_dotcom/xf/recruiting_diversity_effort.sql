WITH issues AS (
  
    SELECT *
    FROM {{ ref ('gitlab_dotcom_issues_xf') }}
    WHERE project_id =16492321 --Recruiting for Open Positions

), users AS (

    SELECT *
    FROM {{ ref ('gitlab_dotcom_users') }}

), assignee AS (

    SELECT 
      assignee.*, 
      user_name AS assignee
    FROM {{ ref ('gitlab_dotcom_issue_assignees') }} assignee
    LEFT JOIN users
      ON assignee.user_id = users.user_id 

), agg_assignee AS (

    SELECT
     issue_id,
     ARRAY_AGG(LOWER(assignee)) WITHIN GROUP (ORDER BY assignee ASC) AS assignee
    FROM assignee
    GROUP BY issue_id

), intermediate AS (

    SELECT 
      issues.issue_title,
      issues.issue_iid,
      issues.issue_created_at,
      DATE_TRUNC(week,issue_created_at)                         AS issue_created_week,
      issues.issue_closed_at,
      DATE_TRUNC(week,issues.issue_closed_at)                   AS issue_closed_week,
      IFF(issue_closed_at IS NOT NULL,1,0)                      AS is_issue_closed,
      issues.state,
      agg_assignee.assignee,
      IFF(CONTAINS(issue_description, '[x] Yes, Diversity Sourcing methods were used'::varchar) = True,
        'Used Diversity Strings',null)                          AS used_diversity_booleanstrings,
      IFF(CONTAINS(issue_description, '[x] No, I did not use Diversity Sourcing methods'::varchar) = True,
        'Did not Use',null)                                     AS did_not_use,
      IFF(used_diversity_booleanstrings is null AND did_not_use is null, 
        'No Answer',NULL)                                       AS No_Answer
    FROM issues
    LEFT JOIN agg_assignee 
      ON agg_assignee.issue_id = issues.issue_id
    WHERE project_id = 16492321
      AND issue_title LIKE '%Weekly Check-in:%'
      AND issue_title NOT LIKE '%Test%'
  
)

SELECT *
FROM intermediate
