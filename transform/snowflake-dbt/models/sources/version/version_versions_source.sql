WITH source AS (

    SELECT *
    FROM {{ source('version', 'versions') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER           AS id,
      verson::VARCHAR       AS version,
      vulnerable::BOOLEAN   AS vulnerable,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at
    FROM source  

)

SELECT *
FROM renamed