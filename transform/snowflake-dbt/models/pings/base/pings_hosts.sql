{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('pings_tap_postgres', 'hosts') }}

),

renamed AS (

  SELECT
    id::INTEGER                         AS id,
    url::VARCHAR                      AS host_url,
    created_at,
    updated_at,
    star,
    fortune_rank,
    in_salesforce,
    current_usage_data_id,
    current_version_check_id

  FROM source

)

SELECT *
FROM renamed
