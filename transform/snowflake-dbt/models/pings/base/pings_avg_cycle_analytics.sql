WITH source AS (

	SELECT *
	FROM {{ var("database") }}.version_db.avg_cycle_analytics

),

    renamed AS (

    SELECT
    -- id
    id,
    usage_data_id  AS ping_id,

    -- data
    code_average,
    code_missing,
    code_sd,

    issue_average,
    issue_missing,
    issue_sd,

    plan_average,
    plan_missing,
    plan_sd,

    production_average,
    production_missing,
    production_sd,

    review_average,
    review_missing,
    review_sd,

    staging_average,
    staging_missing,
    staging_sd,

    test_average,
    test_missing,
    test_sd,

    total

    FROM source

  )

SELECT *
FROM renamed