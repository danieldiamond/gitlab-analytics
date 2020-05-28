WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'clusters_applications_cert_managers') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), 

renamed AS (
    
    SELECT
      id::INTEGER              AS clusters_applications_cert_managers_id,
      cluster_id::INTEGER      AS cluster_id,
      created_at::TIMESTAMP    AS created_at,
      updated_at::TIMESTAMP    AS updated_at,
      status::INTEGER          AS status,
      version::VARCHAR         AS version,
      status_reason::VARCHAR   AS status_reason
    FROM source

)


SELECT *
FROM renamed
