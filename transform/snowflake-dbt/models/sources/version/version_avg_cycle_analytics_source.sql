WITH source AS (

    SELECT *
    FROM {{ source('version', 'avg_cycle_analytics') }}

), renamed AS (

    SELECT
      id::NUMBER                   AS avg_cycle_analytics_id,
      usage_data_id::NUMBER        AS usage_data_id,
      total::NUMBER                AS total_seconds,
      issue_average::NUMBER        AS issue_average_seconds,
      issue_sd::NUMBER             AS issue_standard_deviation_seconds,
      issue_missing::NUMBER        AS issue_missing,
      plan_average::NUMBER         AS plan_average_seconds,
      plan_sd::NUMBER              AS plan_standard_deviation_seconds,
      plan_missing::NUMBER         AS plan_missing,
      code_average::NUMBER         AS code_average_seconds,
      code_sd::NUMBER              AS code_standard_deviation_seconds,
      code_missing::NUMBER         AS code_missing,
      test_average::NUMBER         AS test_average_seconds,
      test_sd::NUMBER              AS test_standard_deviation_seconds,
      test_missing::NUMBER         AS test_missing,
      review_average::NUMBER       AS review_average_seconds,
      review_sd::NUMBER            AS review_standard_deviation_seconds,
      review_missing::NUMBER       AS review_missing,
      staging_average::NUMBER      AS staging_average_seconds,
      staging_sd::NUMBER           AS staging_standard_deviation_seconds,
      staging_missing::NUMBER      AS staging_missing,
      production_average::NUMBER   AS production_average_seconds,
      production_sd::NUMBER        AS production_standard_deviation_seconds,
      production_missing::NUMBER   AS production_missing 
    FROM source

)

SELECT *
FROM renamed
