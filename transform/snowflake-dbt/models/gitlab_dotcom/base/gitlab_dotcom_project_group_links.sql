{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'project_group_links') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), 

{{ distinct_source(source=source('gitlab_dotcom', 'project_group_links'))}}

renamed AS (

    SELECT

      id::INTEGER                                     AS project_group_link_id,
      project_id::INTEGER                             AS project_id,
      group_id::INTEGER                               AS group_id,
      group_access::INTEGER                           AS group_access,
      created_at::TIMESTAMP                           AS project_features_created_at,
      updated_at::TIMESTAMP                           AS project_features_updated_at,
      expires_at::TIMESTAMP                           AS expires_at,
      valid_from -- Column was added in distinct_source CTE

)

{{ scd_type_2(
    primary_key='project_group_link_id',
    primary_key_raw='id',
    source_cte='distinct_source',
    source_timestamp='valid_from',
    casted_cte='renamed'
) }}
