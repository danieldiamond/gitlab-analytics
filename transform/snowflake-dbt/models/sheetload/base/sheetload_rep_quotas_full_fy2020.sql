WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','rep_quotas_full_fy2020') }}
    
), final AS (

    SELECT 
      bamboo_employee_id,
      calendar_month::DATE                                    AS calendar_month,
      fiscal_quarter::INT                                     AS fiscal_quarter,
      fiscal_year::INT                                        AS fiscal_year,
      ZEROIFNULL(NULLIF("FULL_QUOTA",'')::DECIMAL(16,5))      AS full_quota,
      ZEROIFNULL(NULLIF("RAMPING_QUOTA",'')::DECIMAL(16,5))   AS ramping_quota,
      ZEROIFNULL(NULLIF("RAMPING_PERCENT",'')::DECIMAL(3,2))  AS ramping_percent,
      sales_rep,
      team,
      type
    FROM source
      
) 

SELECT * 
FROM final
