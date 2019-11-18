{{ config({
    "materialized": "table"
    })
}}

/*
  The dense rank filters down to only rows added during the last airflow run.
  Waiting on: https://gitlab.com/gitlab-data/analytics/issues/2727
*/
WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'label_links') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
  QUALIFY DENSE_RANK() OVER (ORDER BY _task_instance DESC) = 1

), renamed AS (

    SELECT

      id::INTEGER                                    AS label_link_id,
      label_id::INTEGER                              AS label_id,
      target_id::INTEGER                             AS target_id,
      target_type::VARCHAR                           AS target_type,
      created_at::TIMESTAMP                          AS label_link_created_at,
      updated_at::TIMESTAMP                          AS label_link_updated_at

    FROM source

)

SELECT *
FROM renamed
