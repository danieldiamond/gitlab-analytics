{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'pages_domains') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (
  
      SELECT
        id::INTEGER                             AS pages_domain_id,
        project_id::INTEGER                     AS project_id,
        certificate::VARCHAR                    AS certificate,
        domain::VARCHAR                         AS domain,
        verified_at::TIMESTAMP                  AS verified_at,
        verification_code::VARCHAR              AS verification_code,
        enabled_until::TIMESTAMP                AS enabled_until,
        remove_at::TIMESTAMP                    AS remove_at,
        auto_ssl_enabled::VARCHAR               AS auto_ssl_enabled,
        certificate_valid_not_before::TIMESTAMP AS certificate_valid_not_before,
        certificate_valid_not_after::TIMESTAMP  AS certificate_valid_not_after,
        certificate_source::VARCHAR             AS certificate_source
      FROM source
      
)

SELECT * 
FROM renamed
