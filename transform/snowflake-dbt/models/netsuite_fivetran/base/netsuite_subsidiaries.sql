WITH source AS (

    SELECT *
    FROM {{ source('netsuite_fivetran', 'subsidiaries') }}

), renamed AS (

    SELECT subsidiary_id,
           full_name                 AS subsidiary_full_name,
           name                      AS subsidiary_name,
           base_currency_id,
           isinactive::boolean       AS is_subsidiary_inactive,
           is_elimination::boolean   AS is_elimination_subsidiary

    FROM source

)

SELECT *
FROM renamed
