{{ config({
    "materialized": "view"
    })
}}

WITH category_handbook_engineering_merge_requests AS (

    SELECT *
    FROM {{ ref('category_handbook_engineering_merge_requests') }}

), handbook_engineering_merge_request_path_count_department AS (

    SELECT
        merge_request_state,
        merge_request_updated_at,
        merge_request_created_at,
        merge_request_last_edited_at,
        merge_request_merged_at,
        merge_request_iid,
        merge_request_path,
        CASE WHEN LOWER(merge_request_path) LIKE '%/handbook/engineering/%' THEN 1 
             WHEN LOWER(merge_request_path) LIKE '%/handbook/support/%' THEN 1 
             ELSE 0 END AS path_count_engineering,
        IFF(LOWER(merge_request_path) LIKE '%/handbook/engineering/ux/%',1,0) AS path_count_ux,
        IFF(LOWER(merge_request_path) LIKE '%/handbook/engineering/security/%',1,0) AS path_count_security,
        IFF(LOWER(merge_request_path) LIKE '%/handbook/engineering/infrastructure/%',1,0) AS path_count_infrastructure,
        IFF(LOWER(merge_request_path) LIKE '%/handbook/engineering/development/%',1,0) AS path_count_development,
        IFF(LOWER(merge_request_path) LIKE '%/handbook/engineering/quality/%',1,0) AS path_count_quality,
        IFF(LOWER(merge_request_path) LIKE '%/handbook/support/%',1,0) AS path_count_support
    FROM category_handbook_engineering_merge_requests

)

SELECT *
FROM handbook_engineering_merge_request_path_count_department
