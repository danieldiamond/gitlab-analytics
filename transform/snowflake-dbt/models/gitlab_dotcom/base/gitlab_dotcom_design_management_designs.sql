WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'design_management_designs') }}

), renamed AS (

    SELECT
      MD5(id)::VARCHAR                            AS design_id,
      project_id::INTEGER                         AS project_id,
      issue_id::INTEGER                           AS issue_id,
      filename::VARCHAR                           AS design_filename
    FROM source

)

SELECT *
FROM renamed
