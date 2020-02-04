WITH split_diff_path AS (
    SELECT
      added_lines::bigint                                       AS product_merge_request_lines_added,
      real_size::varchar                                        AS product_merge_request_files_changed,
      REGEXP_REPLACE(real_size::varchar, '[^0-9]+', '')::bigint AS product_merge_request_files_changed_truncated,
      removed_lines::varchar                                    AS product_merge_request_lines_removed,
      SPLIT(plain_diff_url_path, '-')                           AS product_merge_request_diff_url_split,
      ARRAY_SIZE(SPLIT(plain_diff_url_path, '-'))               AS product_merge_request_diff_url_size 
    FROM {{ ref('engineering_part_of_product_merge_requests') }}
)
SELECT 
    product_merge_request_lines_added,
    product_merge_request_files_changed,
    product_merge_request_files_changed_truncated,
    product_merge_request_lines_removed,
    ARRAY_TO_STRING(ARRAY_SLICE(product_merge_request_diff_url_split, 0, -1), '-')::varchar AS product_merge_request_project,
    REGEXP_REPLACE(GET(product_merge_request_diff_url_split, product_merge_request_diff_url_size - 1), '[^0-9]+', '')::bigint AS product_merge_request_id
FROM split_diff_path

