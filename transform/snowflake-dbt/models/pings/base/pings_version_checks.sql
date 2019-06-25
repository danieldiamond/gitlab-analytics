with source AS (

  SELECT * 
  FROM {{ source('pings_tap_postgres', 'version_checks') }}

), renamed AS (

  SELECT  id,
          host_id,

          created_at,
          updated_at,

          gitlab_version,
          referer_url,
          request_data
  FROM source
)

SELECT * 
FROM renamed
