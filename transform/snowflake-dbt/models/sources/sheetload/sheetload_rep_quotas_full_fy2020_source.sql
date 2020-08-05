WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','rep_quotas_full_fy2020') }}
    
), final AS (

    SELECT 
      bamboo_employee_id,
      sfdc_user_id,
      calendar_month::DATE                                              AS calendar_month,
      fiscal_quarter::NUMBER                                               AS fiscal_quarter,
      fiscal_year::NUMBER                                                  AS fiscal_year,
      adjusted_start_date::DATE                                         AS adjusted_start_date,
      ZEROIFNULL(NULLIF(TRIM(full_quota),'NA')::NUMBER(16,5))          AS full_quota,
      ZEROIFNULL(NULLIF(TRIM(ramping_quota),'')::NUMBER(16,5))         AS ramping_quota,
      ZEROIFNULL(NULLIF(TRIM(ramping_percent),'')::NUMBER(3,2))        AS ramping_percent,
      ZEROIFNULL(NULLIF(TRIM(seasonality_percent),'')::NUMBER(3,2))    AS seasonality_percent,
      ZEROIFNULL(NULLIF(TRIM(gross_iacv_attainment),'')::NUMBER(16,2)) AS gross_iacv_attainment,
      ZEROIFNULL(NULLIF(TRIM(net_iacv_attainment),'')::NUMBER(16,2))   AS net_iacv_attainment,
      sales_rep,
      team,
      type
    FROM source
      
) 

SELECT * 
FROM final
