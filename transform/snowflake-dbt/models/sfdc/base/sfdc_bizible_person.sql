{{config({
    "materialized": "table",
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'bizible_person') }}

), renamed AS (

    SELECT
      id                              AS person_id,
      bizible2__lead__c               AS bizible_lead_id,
      bizible2__contact__c            AS bizible_contact_id
      
    FROM source
	WHERE isdeleted = FALSE

)

SELECT *
FROM renamed
