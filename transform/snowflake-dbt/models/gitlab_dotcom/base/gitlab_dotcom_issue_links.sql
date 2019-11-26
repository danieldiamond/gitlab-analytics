
{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'issue_links') }}

),

{{ distinct_source(source=source('gitlab_dotcom', 'issue_links'))}}

, renamed AS (

    SELECT
      id::INTEGER                      AS issue_link_id,
      source_id::INTEGER               AS source_id,
      target_id::INTEGER               AS target_id,
      created_at::TIMESTAMP            AS created_at,
      updated_at::TIMESTAMP            AS updated_at,
      valid_from -- Column was added in distinct_source CTE

    FROM distinct_source

)

{{ scd_type_2(
    primary_key='issue_link_id',
    primary_key_raw='id',
    source_cte='distinct_source',
    source_timestamp='valid_from',
    casted_cte='renamed'
) }}

