WITH source AS (

    SELECT *
    FROM {{ source('netsuite_accounts_source') }}

)

SELECT *
FROM renamed
