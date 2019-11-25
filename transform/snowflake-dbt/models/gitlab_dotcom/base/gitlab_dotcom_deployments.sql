{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *
  FROM {{ source('gitlab_dotcom', 'deployments') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                                      AS deployment_id,
      iid::INTEGER                                     AS deployment_iid,
      project_id::INTEGER                              AS project_id,
      environment_id::INTEGER                          AS environment_id,
      ref::VARCHAR                                     AS ref,
      tag::BOOLEAN                                     AS tag,
      sha::VARCHAR                                     AS sha,
      user_id::INTEGER                                 AS user_id,
      deployable_id::INTEGER                           AS deployable_id,
      deployable_type::VARCHAR                         AS deployable_type,
      created_at::TIMESTAMP                            AS created_at,
      updated_at::TIMESTAMP                            AS updated_at,
      on_stop::VARCHAR                                 AS on_stop,
      finished_at::TIMESTAMP                           AS finished_at,
      status::INTEGER                                  AS status,
      cluster_id::INTEGER                              AS cluster_id

    FROM source

)


SELECT *
FROM renamed
