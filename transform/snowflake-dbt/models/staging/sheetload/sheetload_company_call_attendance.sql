WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_company_call_attendance_source') }}

)

SELECT *
FROM source
