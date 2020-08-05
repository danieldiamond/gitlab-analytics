WITH 
{{ distinct_source(source=source('gitlab_dotcom', 'group_group_links')) }}

, renamed AS (

    SELECT

      id::INTEGER                                     AS group_group_link_id,
      shared_group_id::INTEGER                        AS shared_group_id,
      shared_with_group_id::INTEGER                   AS shared_with_group_id,
      group_access::INTEGER                           AS group_access,
      created_at::TIMESTAMP                           AS created_at,
      updated_at::TIMESTAMP                           AS updated_at,
      expires_at::TIMESTAMP                           AS expires_at,
      valid_from -- Column was added in distinct_source CTE

    FROM distinct_source

)

{{ scd_type_2(
    primary_key_renamed='group_group_link_id',
    primary_key_raw='id'
) }}
