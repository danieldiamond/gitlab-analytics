{{ config({
    "schema": "staging"
    })
}}

WITH 
{{ distinct_source(source=source('gitlab_dotcom', 'project_group_links')) }}

, renamed AS (

    SELECT

      id::INTEGER                                     AS project_group_link_id,
      project_id::INTEGER                             AS project_id,
      group_id::INTEGER                               AS group_id,
      group_access::INTEGER                           AS group_access,
      created_at::TIMESTAMP                           AS created_at,
      updated_at::TIMESTAMP                           AS updated_at,
      expires_at::TIMESTAMP                           AS expires_at,
      valid_from -- Column was added in distinct_source CTE

    FROM distinct_source

)

{{ scd_type_2(
    primary_key_renamed='project_group_link_id',
    primary_key_raw='id'
) }}
