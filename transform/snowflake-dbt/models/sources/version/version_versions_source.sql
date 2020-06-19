WITH source AS (

    SELECT *
    FROM {{ source('version', 'versions') }}

), renamed AS (

    SELECT
      id::INTEGER           AS id,
      version::VARCHAR      AS version,
      vulnerable::BOOLEAN   AS is_vulnerable,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at
    FROM source  

)

SELECT *
FROM renamed