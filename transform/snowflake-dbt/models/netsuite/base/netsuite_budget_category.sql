WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'budget_category') }}

), renamed AS (

    SELECT
      --Primary Key
      budget_category_id::FLOAT             AS budget_category_id,

      --Info
      isinactive::BOOLEAN                   AS is_inactive,
      is_global::BOOLEAN                    AS is_global,
      name::VARCHAR                         AS budget_category

    FROM source
    WHERE LOWER(_fivetran_deleted) = 'false'

)

SELECT *
FROM renamed
