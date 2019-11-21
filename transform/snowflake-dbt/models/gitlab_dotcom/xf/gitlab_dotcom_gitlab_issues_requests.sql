WITH epic_issues AS (

  SELECT *
  FROM {{ref('gitlab_dotcom_epic_issues')}}

)

, epics AS (

  SELECT *
  FROM {{ref('gitlab_dotcom_epics')}}

)

, gitlab_dotcom_issues_linked_to_sfdc_account_id AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_issues_linked_to_sfdc_account_id') }}

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

  SELECT
    DISTINCT
    issues.issue_id,
    issues.issue_iid,
    issues.issue_title,
    issues.issue_created_at,
    issues.milestone_id,
    issues.issue_state,
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

, sfdc_account_issues_from_issues AS (

  SELECT
    DISTINCT
    issues.issue_id,
    issues.issue_iid,
    issues.issue_title,
    issues.issue_created_at,
    issues.milestone_id,
    issues.issue_state,
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

  FROM gitlab_dotcom_issues_linked_to_sfdc_account_id
  LEFT JOIN issues
    ON gitlab_dotcom_issues_linked_to_sfdc_account_id.issue_id = issues.issue_id
  LEFT JOIN projects
    ON issues.project_id = projects.project_id
  LEFT JOIN namespaces
    ON projects.namespace_id = namespaces.namespace_id
  LEFT JOIN sfdc_accounts
    ON gitlab_dotcom_issues_linked_to_sfdc_account_id.sfdc_account_id = sfdc_accounts.account_id
  LEFT JOIN epic_issues
    ON issues.issue_id = epic_issues.issue_id
  LEFT JOIN epics
    ON epic_issues.epic_id = epics.epic_id
)

, unioned AS (
      
  SELECT *
  FROM sfdc_account_issues_from_notes
  
  UNION 
    
  SELECT *
  FROM sfdc_account_issues_from_issues
    
)

SELECT *
FROM unioned
