{{ config({
    "materialized": "view"
    })
}}

WITH category_handbook_engineering_merge_requests_count AS (

    SELECT *
    FROM {{ ref('category_handbook_engineering_merge_requests_count') }}

), handbook_engineering_total_count_department AS (

    SELECT
        merge_request_state,
        DATE_TRUNC('MONTH', merge_request_merged_at) AS month_merged_at,
        SUM(mr_total_count_engineering) AS total_count_engineering,
        SUM(mr_total_count_ux) AS total_count_ux,
        SUM(mr_total_count_security) AS total_count_security,
        SUM(mr_total_count_infrastructure) AS total_count_infrastructure,
        SUM(mr_total_count_development) AS total_count_development,
        SUM(mr_total_count_quality) AS total_count_quality,
        SUM(mr_total_count_support) AS total_count_support
    FROM category_handbook_engineering_merge_requests_count
    {{ dbt_utils.group_by(n=2) }}

)

SELECT *
FROM handbook_engineering_total_count_department
