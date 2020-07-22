WITH source AS (

    SELECT *
    FROM {{ source('license', 'granted_add_ons') }}

), renamed AS (

    SELECT
      id::INTEGER            AS granted_add_on_id,
      license_id::INTEGER    AS license_id,
      add_on_id::INTEGER     AS add_on_id,
      quantity::INTEGER      AS quantity,
      created_at::TIMESTAMP  AS created_at,
      updated_at::TIMESTAMP  AS updated_at
    FROM source

)

SELECT *
FROM renamed
