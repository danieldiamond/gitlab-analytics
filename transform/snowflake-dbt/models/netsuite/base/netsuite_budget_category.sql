WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'budget_category') }}

), renamed AS (

    SELECT --Primary Key
           budget_category_id::FLOAT             AS budget_category_id,

           --Info
           isinactive::BOOLEAN                   AS is_inactive,
           is_global::BOOLEAN                    AS is_global,
           name::VARCHAR                         AS budget_name

    FROM source
    WHERE _fivetran_deleted = 'False'

)

SELECT *
FROM renamed
