WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('version', 'hosts') }}

),

renamed AS (

  SELECT
    id::INTEGER                         AS host_id,
    url::VARCHAR                        AS host_url,
    created_at::TIMESTAMP               AS created_at,
    updated_at::TIMESTAMP               AS updated_at,
    star::BOOLEAN                       AS has_star,
    fortune_rank::INTEGER               AS fortune_rank
  FROM source
  WHERE rank_in_key = 1

)

SELECT *
FROM renamed
