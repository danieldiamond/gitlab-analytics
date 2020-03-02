WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','kpi_status') }}
    
), final AS (

    SELECT 
        NULLIF("kpi_grouping", '')::VARCHAR             AS kpi_grouping,
        NULLIF("kpi_sub_grouping", '')::VARCHAR         AS kpi_sub_grouping,
        NULLIF("kpi", '')::VARCHAR                      AS kpi_name,
        NULLIF("start_date", '')::VARCHAR::DATE         AS start_date,
        NULLIF("completion_date", '')::VARCHAR::DATE    AS completion_date,
        NULLIF("status", '')::VARCHAR                   AS status,
        NULLIF("comment", '')::VARCHAR                  AS comment,
        NULLIF("in_handbook", '')::VARCHAR::BOOLEAN     AS in_handbook,
        NULLIF("sisense_link", '')::VARCHAR             AS sisense_link,
        NULLIF("gitlab_issue", '')::VARCHAR             AS gitlab_issue,
        NULLIF("commit_start", '')::VARCHAR             AS commit_start,
        NULLIF("commit_handbook_v1", '')::VARCHAR       AS commit_handbook_v1,
        NULLIF("is_deleted", '')::VARCHAR::BOOLEAN     AS is_deleted
    FROM source
      
) 

SELECT * 
FROM final
