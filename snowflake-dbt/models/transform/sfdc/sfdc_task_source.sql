WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'task') }}

), renamed AS(

    SELECT
    id             AS task_id,
        
        --keys
    accountid      AS account_id,
    ownerid        AS owner_id,
    whoid          AS lead_or_contact_id,
        
        --info      
    subject        AS task_subject,
    activitydate   AS task_date,
    isdeleted      AS is_deleted

    FROM source
)

SELECT *
FROM renamed
