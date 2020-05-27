WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'elasticsearch_indexed_namespaces') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY UPDATED_AT DESC) = 1

), types_cast AS (

    SELECT
      namespace_id::INTEGER     AS namespace_id,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at
    FROM source

)

SELECT *
FROM types_cast