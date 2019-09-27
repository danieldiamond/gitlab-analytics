WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'currencies') }}

), renamed AS (

    SELECT currency_id::float             AS currency_id,
           name::varchar                  AS currency_name,
           precision_0::float             AS decimal_precision,
           symbol::varchar                AS currency_symbol,
           is_inactive::boolean           AS is_currency_inactive

    FROM source
    WHERE _fivetran_deleted = 'False'

)

SELECT *
FROM renamed
