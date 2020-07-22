WITH source AS (

    SELECT * 
    FROM {{ ref('sheetload_hire_replan_source') }}

) 

SELECT *
FROM source
