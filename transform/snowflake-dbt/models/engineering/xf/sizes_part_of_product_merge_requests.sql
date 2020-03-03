WITH split_diff_path AS (

    SELECT
      added_lines::BIGINT                                       AS product_merge_request_lines_added,
      real_size::VARCHAR                                        AS product_merge_request_files_changed,
      REGEXP_REPLACE(real_size::VARCHAR, '[^0-9]+', '')::BIGINT AS product_merge_request_files_changed_truncated,
      removed_lines::VARCHAR                                    AS product_merge_request_lines_removed,
      SPLIT(plain_diff_url_path, '-')                           AS product_merge_request_diff_url_split,
      ARRAY_SIZE(SPLIT(plain_diff_url_path, '-'))               AS product_merge_request_diff_url_size 
    FROM {{ ref('engineering_part_of_product_merge_requests') }}

), id_split_out AS (

    SELECT 
      product_merge_request_lines_added,
      product_merge_request_files_changed,
      product_merge_request_files_changed_truncated,
      product_merge_request_lines_removed,
      TRIM(ARRAY_TO_STRING(ARRAY_SLICE(product_merge_request_diff_url_split, 0, -1), '-'), '/')::VARCHAR AS product_merge_request_project,
      REGEXP_REPLACE(GET(product_merge_request_diff_url_split, product_merge_request_diff_url_size - 1), '[^0-9]+', '')::bigint AS product_merge_request_id
    FROM split_diff_path

), project_id_merged_in AS (

    SELECT
      product_merge_request_lines_added,
      product_merge_request_files_changed,
      product_merge_request_files_changed_truncated,
      product_merge_request_lines_removed,
      product_merge_request_project
      product_projects.project_id AS product_merge_request_project_id,
      product_merge_request_iid
    FROM id_split_out
    INNER JOIN {{ ref('projects_part_of_product') }} product_projects
      ON product_projects.project_path = id_split_out.project_path

)
SELECT * 
FROM id_split_out


