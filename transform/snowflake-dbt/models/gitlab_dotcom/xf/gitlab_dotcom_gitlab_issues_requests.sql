WITH epic_issues AS (

  SELECT *
  FROM {{ref('gitlab_dotcom_epic_issues')}}

)

, epics AS (

  SELECT *
  FROM {{ref('gitlab_dotcom_epics_xf')}}

)

, gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id') }}

)

, gitlab_dotcom_notes_linked_to_sfdc_account_id AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_notes_linked_to_sfdc_account_id') }}

)

, issues AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_issues_xf') }}

)

, projects AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_projects') }}

)

, namespaces AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespaces_xf') }}
)

, sfdc_accounts AS (

  SELECT *
  FROM {{ ref('sfdc_accounts_xf') }}
)

, sfdc_opportunities AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity_xf') }}
)

, sfdc_account_issues_from_notes AS (

  SELECT DISTINCT
    'Issue'                    AS noteable_type,
    issues.issue_id            AS noteable_id,
    issues.issue_iid           AS noteable_iid,
    issues.issue_title         AS noteable_title,
    issues.issue_created_at    AS noteable_created_at,
    issues.milestone_id,
    issues.state               AS noteable_state,
    issues.weight,
    issues.labels,
    projects.project_name,
    projects.project_id,
    namespaces.namespace_id,
    namespaces.namespace_name,
    sfdc_accounts.account_id   AS sfdc_account_id,
    sfdc_accounts.account_type AS sfdc_account_type,
    sfdc_accounts.total_account_value,
    sfdc_accounts.carr_total,
    sfdc_accounts.count_licensed_users,
    epics.epic_title

  FROM gitlab_dotcom_notes_linked_to_sfdc_account_id
  INNER JOIN issues
    ON gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_id = issues.issue_id
  LEFT JOIN projects
    ON issues.project_id = projects.project_id
  LEFT JOIN namespaces
    ON projects.namespace_id = namespaces.namespace_id
  LEFT JOIN sfdc_accounts
    ON gitlab_dotcom_notes_linked_to_sfdc_account_id.sfdc_account_id = sfdc_accounts.account_id
  LEFT JOIN epic_issues
    ON issues.issue_id = epic_issues.issue_id
  LEFT JOIN epics
    ON epic_issues.epic_id = epics.epic_id
  WHERE gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_type = 'Issue'
)

, sfdc_account_epics_from_notes AS (

  SELECT DISTINCT
    'Epic'                     AS noteable_type,
    epics.epic_id              AS noteable_id,
    epics.epic_internal_id     AS noteable_iid,
    epics.epic_title           AS noteable_title,
    epics.created_at           AS noteable_created_at,
    NULL                       AS milestone_id,
    epics.state                AS epic_state,
    NULL                       AS weight,
    epics.labels               AS labels,
    NULL                       AS project_name,
    NULL                       AS project_id,
    namespaces.namespace_id,
    namespaces.namespace_name,
    sfdc_accounts.account_id   AS sfdc_account_id,
    sfdc_accounts.account_type AS sfdc_account_type,
    sfdc_accounts.total_account_value,
    sfdc_accounts.carr_total,
    sfdc_accounts.count_licensed_users,
    epics.epic_title --Redundant in this case.

  FROM gitlab_dotcom_notes_linked_to_sfdc_account_id
  INNER JOIN epics
    ON gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_id = epics.epic_id
  LEFT JOIN namespaces
    ON epics.group_id = namespaces.namespace_id
  LEFT JOIN sfdc_accounts
    ON gitlab_dotcom_notes_linked_to_sfdc_account_id.sfdc_account_id = sfdc_accounts.account_id
  WHERE gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_type = 'Epic'
)

, sfdc_account_issues_from_issues AS (

  SELECT
    DISTINCT
    issues.issue_id,
    issues.issue_iid,
    issues.issue_title,
    issues.issue_created_at,
    issues.milestone_id,
    issues.state AS issue_state,
    issues.weight,
    issues.masked_label_title,
    projects.project_name,
    projects.project_id,
    namespaces.namespace_id,
    namespaces.namespace_name,
    sfdc_accounts.account_id AS sfdc_account_id,
    sfdc_accounts.account_type AS sfdc_account_type,
    sfdc_accounts.total_account_value,
    sfdc_accounts.carr_total,
    sfdc_accounts.count_licensed_users,
    epics.epic_title

  FROM gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id
  LEFT JOIN issues
    ON gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.issue_id = issues.issue_id
  LEFT JOIN projects
    ON issues.project_id = projects.project_id
  LEFT JOIN namespaces
    ON projects.namespace_id = namespaces.namespace_id
  LEFT JOIN sfdc_accounts
    ON gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.sfdc_account_id = sfdc_accounts.account_id
  LEFT JOIN epic_issues
    ON issues.issue_id = epic_issues.issue_id
  LEFT JOIN epics
    ON epic_issues.epic_id = epics.epic_id
)

, unioned AS (
  
  /* Notes */
  SELECT *
  FROM sfdc_account_issues_from_notes

  UNION 

  SELECT *
  FROM sfdc_account_epics_from_notes
  
  /* Descriptions */
  UNION 
    
  SELECT *
  FROM sfdc_account_issues_from_issues
    
)

SELECT *
FROM unioned
