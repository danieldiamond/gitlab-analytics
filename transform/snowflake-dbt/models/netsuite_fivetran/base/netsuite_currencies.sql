WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'currencies') }}

), renamed AS (

    SELECT currency_id,
           name                  AS currency_name,
           precision_0           AS decimal_precision,
           symbol                AS currency_symbol,
           is_inactive::boolean  AS is_currency_inactive

    FROM source
    WHERE _fivetran_deleted = 'False'

)

SELECT *
FROM renamed
