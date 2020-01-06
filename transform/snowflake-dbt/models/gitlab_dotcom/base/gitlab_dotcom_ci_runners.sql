{{ config({
    "materialized": "incremental",
    "unique_key": "runner_id",
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_runners') }}
  WHERE created_at IS NOT NULL

    {% if is_incremental() %}

    AND updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

  SELECT
    id::INTEGER                         AS runner_id,
    created_at::TIMESTAMP               AS created_at,
    updated_at::TIMESTAMP               AS updated_at,
    description::VARCHAR                AS description,
    contacted_at::TIMESTAMP             AS contacted_at,
    active::BOOLEAN                     AS is_active,
    is_shared::BOOLEAN                  AS is_shared,
    name::VARCHAR                       AS runner_name,
    version::VARCHAR                    AS version,
    revision::VARCHAR                   AS revision,
    platform::VARCHAR                   AS platform,
    architecture::VARCHAR               AS architecture,
    run_untagged::BOOLEAN               AS is_untagged,
    locked::BOOLEAN                     AS is_locked,
    access_level::INTEGER               AS access_level,
    ip_address::VARCHAR                 AS ip_address,
    maximum_timeout::INTEGER            AS maximum_timeout,
    runner_type::INTEGER                AS runner_type
  FROM source

)

SELECT *
FROM renamed
ORDER BY updated_at
