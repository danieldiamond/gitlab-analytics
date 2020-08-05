WITH source AS (

    SELECT *
    FROM {{ source('license', 'add_ons') }}

), renamed AS (

    SELECT
      id::NUMBER             AS add_on_id,
      name::VARCHAR           AS add_on_name,
      code::VARCHAR           AS add_on_code,
      created_at::TIMESTAMP   AS created_at,
      updated_at::TIMESTAMP   AS updated_at
    FROM source

)

SELECT *
FROM renamed
