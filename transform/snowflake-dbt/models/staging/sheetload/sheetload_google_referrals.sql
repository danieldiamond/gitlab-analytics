WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_google_referrals_source') }}

)

SELECT *
FROM source