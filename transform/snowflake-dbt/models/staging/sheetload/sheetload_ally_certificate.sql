WITH source AS (

    SELECT * 
    FROM {{ ref('sheetload_ally_certificate_source') }}
),
{{cleanup_certificates_original("'ally_certificate', 'submitter_email'")}}
