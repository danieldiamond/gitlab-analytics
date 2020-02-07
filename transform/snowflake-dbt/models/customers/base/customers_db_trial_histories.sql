WITH source AS (

    SELECT
      *
    FROM {{ source('customers', 'customers_db_trial_histories') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY gl_namespace_id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT DISTINCT
      gl_namespace_id::INTEGER AS gl_namespace_id,
      start_date::TIMESTAMP    AS start_date,
      expired_on::TIMESTAMP    AS expired_on,
      created_at::TIMESTAMP    AS created_at,
      updated_at::TIMESTAMP    AS updated_at,
      glm_source::VARCHAR      AS glm_source,
      glm_content::VARCHAR     AS glm_content,
      trial_entity::VARCHAR    AS trial_entity
    FROM source
    
)

SELECT *
FROM renamed
