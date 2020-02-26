WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','kpi_status') }}
    
), final AS (

    SELECT 
        kpi_grouping,
        kpi_sub_grouping,
        kpi,
        start_month,
        completion_month,
        status,
        comment,
        in_handbook,
        sisense_link,
        gitlab_issue,
        commit_start,
        commit_handbook_v1
    FROM source
      
) 

SELECT * 
FROM final
