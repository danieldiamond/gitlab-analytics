WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'deployments') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                                      AS deployment_id,
      iid::NUMBER                                     AS deployment_iid,
      project_id::NUMBER                              AS project_id,
      environment_id::NUMBER                          AS environment_id,
      ref::VARCHAR                                     AS ref,
      tag::BOOLEAN                                     AS tag,
      sha::VARCHAR                                     AS sha,
      user_id::NUMBER                                 AS user_id,
      deployable_id::NUMBER                           AS deployable_id,
      deployable_type::VARCHAR                         AS deployable_type,
      created_at::TIMESTAMP                            AS created_at,
      updated_at::TIMESTAMP                            AS updated_at,
      on_stop::VARCHAR                                 AS on_stop,
      finished_at::TIMESTAMP                           AS finished_at,
      status::NUMBER                                  AS status_id,
      cluster_id::NUMBER                              AS cluster_id
    FROM source

)

SELECT *
FROM renamed
