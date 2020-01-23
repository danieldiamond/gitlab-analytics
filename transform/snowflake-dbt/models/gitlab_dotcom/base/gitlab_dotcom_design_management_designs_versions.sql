WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'design_management_designs') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      MD5(id)                                     AS design_version_id,
      design_id::VARCHAR                          AS design_id,
      version_id::INTEGER                         AS project_id,
      event::INTEGER                              AS event_type_id
    FROM source

)

SELECT *
FROM renamed
