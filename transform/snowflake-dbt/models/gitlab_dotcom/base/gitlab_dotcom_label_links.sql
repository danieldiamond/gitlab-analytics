{{ config({
    "materialized": "table"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'label_links') }}

),

{{ distinct_source(source=source('gitlab_dotcom', 'label_links'))}}

, renamed AS (

    SELECT

      id::INTEGER                                    AS label_link_id,
      label_id::INTEGER                              AS label_id,
      target_id::INTEGER                             AS target_id,
      target_type::VARCHAR                           AS target_type,
      created_at::TIMESTAMP                          AS label_link_created_at,
      updated_at::TIMESTAMP                          AS label_link_updated_at,
      valid_from -- Column was added in distinct_source CTE

    FROM distinct_source

)

{{ scd_type_2(
    primary_key='label_link_id',
    primary_key_raw='id',
    source_cte='distinct_source',
    source_timestamp='valid_from',
    casted_cte='renamed'
) }}
