WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'clari_export_forecast_net_iacv') }}

), renamed as (

    SELECT
         "User"::VARCHAR                          AS user,
         "Email"::varchar                         AS email,
         "CRM_User_ID"::varchar                   AS crm_user_id,
         "Role"::varchar                          AS sales_team_role,
         "Parent_Role"::varchar                   AS parent_role,
         "Timeframe"::varchar                     AS timeframe,
         "Field"::varchar                         AS field,
         "Week"::INTEGER                          AS week,
         "Start_Day"::DATE                        AS start_date,
         "End_Day"::DATE                          AS end_day,
         NULLIF("Data_Type"::varchar)             AS data_type,
         NULLIF("Data_Value"::varchar)            AS data_value
    FROM source

)

SELECT *
FROM renamed
