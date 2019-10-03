WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'entity') }}

), renamed AS (

    SELECT
      --Primary Key
      entity_id::FLOAT             AS entity_id,

      --Info
      name::VARCHAR                AS entity_name,
      full_name::VARCHAR           AS entity_full_name

    FROM source
    WHERE LOWER(_fivetran_deleted) = 'false'

)

SELECT *
FROM renamed
