WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_eulas') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER           AS eula_id,
      name::VARCHAR         AS eula_name,
      content::VARCHAR      AS eula_content,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at
    FROM source  

)

SELECT *
FROM renamed
