{{config({
    "schema": "staging"
  })
}}

WITH source AS (

  SELECT *
  FROM {{ source('salesforce', 'proof_of_concept') }}




), renamed AS (

  SELECT
  id                                        AS poc_id,
  name                                      AS poc_name,

  -- keys
  account__c                                AS account_id,
  opportunity__c                            AS opportunity_id,
  poc_owner__c                              AS poc_owner_id,
  solutions_architect__c                    AS solutions_architect_id,
  technical_account_manager__c              AS technical_account_manager_id,

  -- dates
  poc_start_date__c                         AS poc_start_date,
  poc_close_date__c                         AS poc_close_date,

  -- info
  decline_reason__c                         AS reason_for_decline,
  general_notes__c                          AS general_notes,
  poc_length__c                             AS poc_length,
  poc_milestone_in_collaboration_project__c AS link_to_gitlab_milestone,
  poc_type__c                               AS poc_type,
  result__c                                 AS poc_result,
  status__c                                 AS poc_status,
  success_criteria__c                       AS success_criteria,
  unsuccessful_reason__c                    AS unsuccessful_reason,

  -- metadata
  createdbyid                               AS created_by_id,
  createddate                               AS created_date,
  lastmodifiedbyid                          AS last_modified_by_id,
  lastmodifieddate                          AS last_modified_date

    FROM source

)

SELECT *
FROM renamed
WHERE poc_id NOT IN (
    'a5v4M000001DZWfQAO' -- https://gitlab.com/gitlab-data/analytics/issues/2516
  , 'a5v4M000001DZWkQAO' -- https://gitlab.com/gitlab-data/analytics/issues/2615
)