WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'approval_merge_request_rules') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

  SELECT
    id::INTEGER                     AS approval_merge_request_rule_id,
    merge_request_id::INTEGER       AS merge_request_id,
    approvals_required::INTEGER     AS is_approvals_required,
    code_owner::BOOLEAN             AS has_code_owner,
    rule_type::VARCHAR              AS rule_type,
    report_type::VARCHAR            AS report_type,
    created_at::TIMESTAMP           AS created_at,
    updated_at::TIMESTAMP           AS updated_at

  FROM source

)

SELECT *
FROM renamed
