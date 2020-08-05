WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'saml_providers') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                              AS saml_provider_id,
      group_id::INTEGER                        AS group_id,
      enabled::BOOLEAN                         AS is_enabled,
      certificate_fingerprint::VARCHAR         AS certificate_fingerprint,
      sso_url::VARCHAR                         AS sso_url,
      enforced_sso::BOOLEAN                    AS is_enforced_sso,
      enforced_group_managed_accounts::BOOLEAN AS is_enforced_group_managed_accounts,
      prohibited_outer_forks::BOOLEAN          AS is_prohibited_outer_forks
    FROM source

)

SELECT *
FROM renamed
