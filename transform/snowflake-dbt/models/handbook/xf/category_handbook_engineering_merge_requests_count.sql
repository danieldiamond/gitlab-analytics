{{ config({
    "materialized": "view"
    })
}}

WITH category_handbook_engineering_merge_requests_path_count AS (

    SELECT *
    FROM {{ ref('category_handbook_engineering_merge_requests_path_count') }}

), handbook_engineering_merge_request_count_department AS (

    SELECT
        merge_request_state,
        merge_request_updated_at,
        merge_request_created_at,
        merge_request_last_edited_at,
        merge_request_merged_at,
        merge_request_iid,
        SUM(path_count_engineering) AS mr_total_count_engineering,
        SUM(path_count_ux) AS mr_total_count_ux,
        SUM(path_count_security) AS mr_total_count_security,
        SUM(path_count_infrastructure) AS mr_total_count_infrastructure,
        SUM(path_count_development) AS mr_total_count_development,
        SUM(path_count_quality) AS mr_total_count_quality,
        SUM(path_count_support) AS mr_total_count_support
    FROM category_handbook_engineering_merge_requests_path_count
    {{ dbt_utils.group_by(n=6) }}

)

SELECT *
FROM handbook_engineering_merge_request_count_department
