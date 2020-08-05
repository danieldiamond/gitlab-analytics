WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'clusters_applications_elastic_stacks') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), 

renamed AS (
    
    SELECT
      id::NUMBER              AS clusters_applications_elastic_stacks_id,
      cluster_id::NUMBER      AS cluster_id,
      created_at::TIMESTAMP    AS created_at,
      updated_at::TIMESTAMP    AS updated_at,
      status::NUMBER          AS status,
      version::VARCHAR         AS version,
      status_reason::VARCHAR   AS status_reason
    FROM source

)


SELECT *
FROM renamed
