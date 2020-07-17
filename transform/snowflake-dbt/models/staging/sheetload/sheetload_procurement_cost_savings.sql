WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_procurement_cost_savings_source') }}

)

SELECT *
FROM source