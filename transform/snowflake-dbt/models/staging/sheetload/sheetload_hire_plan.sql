WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_hire_plan_source') }}

)

SELECT *
FROM source