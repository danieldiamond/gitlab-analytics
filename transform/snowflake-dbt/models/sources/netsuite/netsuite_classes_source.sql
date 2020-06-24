WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'classes') }}

), renamed AS (

    SELECT
      --Primary Key
      class_id::FLOAT              AS class_id,

      --Info
      name::VARCHAR                AS class_name,
      full_name::VARCHAR           AS class_full_name,
      isinactive::BOOLEAN          AS is_inactive

    FROM source
    WHERE _fivetran_deleted = FALSE

)

SELECT *
FROM renamed
