WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','account_golden_records') }}

)

SELECT * 
FROM source
