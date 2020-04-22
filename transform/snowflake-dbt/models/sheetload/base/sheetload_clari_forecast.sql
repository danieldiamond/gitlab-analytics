WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'clari_forecast') }}

), renamed as (

    SELECT
         "User"::varchar                          AS user,
         "Email"::varchar                         AS email,
         "CRM_User_ID"::varchar                   AS crm_user_id
         "Role"::varchar                          AS role
         "Parent_Role"::varchar                   AS parent_role
         "Timeframe"::varchar                     AS timeframe
         "Field"::varchar                         AS field
         "Week"::INTEGER                          AS week
         "Start_Day"::DATE                        AS start_day
         "End_Day"::DATE                          AS end_day
         "Data_Type"::varchar                     AS data_type
         "Data_Value"::varchar                    AS data_value
    FROM source
)

SELECT *
FROM renamed
