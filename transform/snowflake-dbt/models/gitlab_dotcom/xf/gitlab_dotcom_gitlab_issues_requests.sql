{{ config({
    "schema": "analytics"
    })
}}

WITH gitlab_dotcom_notes_linked_to_sfdc_account_id AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_notes_linked_to_sfdc_account_id') }}

)
, issues AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_issues') }}

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

, sfdc_account_issues AS (

  SELECT
    DISTINCT
    issues.issue_id,
    issues.issue_iid,
    issues.title,
    issues.issue_created_at,
    issues.milestone_id,
    issues.state,
    issues.weight,
    projects.project_name,
    projects.project_id,
    namespaces.namespace_id,
    namespaces.namespace_name,
    sfdc_accounts.account_id AS sfdc_account_id,
    sfdc_accounts.account_type AS sfdc_account_type,
    sfdc_accounts.total_account_value,
    sfdc_accounts.carr_total,
    sfdc_accounts.count_licensed_users

  FROM gitlab_dotcom_notes_linked_to_sfdc_account_id
  LEFT JOIN issues
    ON gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_id = issues.issue_id
  LEFT JOIN projects
    ON issues.project_id = projects.project_id
  LEFT JOIN namespaces
    ON projects.namespace_id = namespaces.namespace_id
  LEFT JOIN sfdc_accounts
    ON gitlab_dotcom_notes_linked_to_sfdc_account_id.sfdc_account_id = sfdc_accounts.account_id

  WHERE gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_type = 'Issue'
)

SELECT *
FROM sfdc_account_issues
