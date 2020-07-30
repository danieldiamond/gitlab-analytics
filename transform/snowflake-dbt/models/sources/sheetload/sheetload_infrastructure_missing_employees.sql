WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','infrastructure_missing_employees') }}

), final AS (
    
    SELECT 
        NULLIF(employee_id, '')::INTEGER                                    AS email_domain,
        NULLIF(gitlab_dotcom_user_id, '')::VARCHAR                          AS account_creation_date,
        NULLIF(full_name, '')::VARCHAR                                      AS account_creation_time,
        NULLIF(work_email, '')::VARCHAR                                     AS account_creation_timestamp,
    FROM source

) 

SELECT * 
FROM final
