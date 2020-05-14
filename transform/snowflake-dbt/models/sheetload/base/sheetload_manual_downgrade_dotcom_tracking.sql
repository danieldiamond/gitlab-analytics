WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'manual_downgrade_dotcom_tracking') }}

), renamed AS (

    SELECT
      namespace_id::INTEGER AS namespace_id,
      downgraded_date::DATE AS downgraded_date
    FROM source  

)

SELECT *
FROM renamed