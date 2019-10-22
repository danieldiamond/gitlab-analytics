{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_runners') }}
  WHERE created_at IS NOT NULL

    {% if is_incremental() %}

    AND updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), renamed AS (

  SELECT
    id::INTEGER                         AS id,
    token::VARCHAR                      AS token,
    created_at::TIMESTAMP               AS created_at,
    updated_at::TIMESTAMP               AS updated_at,
    description::VARCHAR                AS description,
    contacted_at::TIMESTAMP             AS contacted_at,
    active::BOOLEAN                     AS active,
    is_shared::BOOLEAN                  AS is_shared,
    name::VARCHAR                       AS name,
    version::VARCHAR                    AS version,
    revision::VARCHAR                   AS revision,
    platform::VARCHAR                   AS platform,
    architecture::VARCHAR               AS architecture,
    run_untagged::BOOLEAN               AS run_untagged,
    locked::BOOLEAN                     AS locked,
    access_level::INTEGER               AS access_level,
    ip_address::VARCHAR                 AS ip_address,
    maximum_timeout::INTEGER            AS maximum_timeout,
    runner_type::BOOLEAN                AS runner_type,
    token_encrypted::VARCHAR            AS token_encrypted 
  FROM source
  WHERE rank_in_key = 1

)

SELECT *
FROM renamed
ORDER BY updated_at
