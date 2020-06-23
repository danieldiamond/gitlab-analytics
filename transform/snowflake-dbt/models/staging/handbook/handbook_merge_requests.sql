WITH source AS (

    SELECT *
    FROM ref( {{ 'handbook_merge_requests_source' }} )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plain_diff_url_path
      ORDER BY ARRAY_SIZE(merge_request_version_diffs) DESC, uploaded_at DESC) = 1

)

SELECT * 
FROM source
 