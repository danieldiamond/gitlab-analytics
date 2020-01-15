WITH source AS (

  SELECT *
  FROM {{ source('version', 'avg_cycle_analytics') }}

),

renamed AS (

  SELECT
    id::INTEGER                   AS avg_cycle_analytics_id,
    usage_data_id::INTEGER        AS usage_data_id,
    total::INTEGER                AS total_seconds,
    issue_average::INTEGER        AS issue_average_seconds,
    issue_sd::INTEGER             AS issue_standard_deviation_seconds,
    issue_missing::INTEGER        AS issue_missing,
    plan_average::INTEGER         AS plan_average_seconds,
    plan_sd::INTEGER              AS plan_standard_deviation_seconds,
    plan_missing::INTEGER         AS plan_missing,
    code_average::INTEGER         AS code_average_seconds,
    code_sd::INTEGER              AS code_standard_deviation_seconds,
    code_missing::INTEGER         AS code_missing,
    test_average::INTEGER         AS test_average_seconds,
    test_sd::INTEGER              AS test_standard_deviation_seconds,
    test_missing::INTEGER         AS test_missing,
    review_average::INTEGER       AS review_average_seconds,
    review_sd::INTEGER            AS review_standard_deviation_seconds,
    review_missing::INTEGER       AS review_missing,
    staging_average::INTEGER      AS staging_average_seconds,
    staging_sd::INTEGER           AS staging_standard_deviation_seconds,
    staging_missing::INTEGER      AS staging_missing,
    production_average::INTEGER   AS production_average_seconds,
    production_sd::INTEGER        AS production_standard_deviation_seconds,
    production_missing::INTEGER   AS production_missing
    
  FROM source

)

SELECT *
FROM renamed
