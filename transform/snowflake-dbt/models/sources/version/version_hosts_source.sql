WITH source AS (

    SELECT *
    FROM {{ source('version', 'hosts') }}

), renamed AS (

    SELECT
      id::NUMBER                AS host_id,
      url::VARCHAR              AS host_url,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at,
      star::BOOLEAN             AS has_star,
      fortune_rank::NUMBER      AS fortune_rank
    FROM source

)

SELECT *
FROM renamed
