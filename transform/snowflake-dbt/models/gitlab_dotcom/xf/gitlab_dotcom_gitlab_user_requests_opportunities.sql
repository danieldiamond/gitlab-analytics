WITH issues_and_epics AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id') }}
    WHERE link_type = 'Opportunity'

), notes AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_notes_linked_to_sfdc_account_id') }}
    WHERE link_type = 'Opportunity'

), sfdc_opportunities AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_xf') }}

), user_requests AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_gitlab_user_requests') }}

), issues_epics_notes_unioned AS (

    SELECT DISTINCT
      noteable_id,
      noteable_type,
      link_id AS opportunity_id,
      sfdc_account_id
    FROM issues_and_epics

    UNION

    SELECT DISTINCT
      noteable_id,
      noteable_type,
      link_id AS opportunity_id,
      sfdc_account_id
    FROM notes    

), joined AS (

    SELECT
      user_requests.*,
      sfdc_opportunities.incremental_acv,
      sfdc_opportunities.opportunity_id AS sfdc_opportunity_id,
      sfdc_opportunities.opportunity_name,
      sfdc_opportunities.sales_type,
      sfdc_opportunities.stage_name
    FROM issues_epics_notes_unioned
    INNER JOIN user_requests
      ON issues_epics_notes_unioned.noteable_id = user_requests.noteable_id
      AND issues_epics_notes_unioned.noteable_type = user_requests.noteable_type
      AND issues_epics_notes_unioned.sfdc_account_id = user_requests.sfdc_account_id 
    INNER JOIN sfdc_opportunities
      ON issues_epics_notes_unioned.opportunity_id = sfdc_opportunities.opportunity_id  

)

SELECT *
FROM joined

