{{ config({
    "materialized": "table"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'label_links') }}
  --QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::INTEGER                                    AS label_link_id,
      label_id::INTEGER                              AS label_id,
      target_id::INTEGER                             AS target_id,
      target_type::VARCHAR                           AS target_type,
      created_at::TIMESTAMP                          AS label_link_created_at,
      updated_at::TIMESTAMP                          AS label_link_updated_at

    FROM source
    WHERE _task_instance IN (SELECT MAX(_task_instance) FROM source)
)

SELECT *
FROM renamed
