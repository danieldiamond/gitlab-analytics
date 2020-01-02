WITH source AS (

    SELECT m.*, date_trunc("day", uploaded_at)
    FROM {{ source('bamboohr', 'id_employee_number_mapping') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC("day", uploaded_at) ORDER BY uploaded_at DESC) = 1

), intermediate AS (

    SELECT 
          nullif(d.value['employeeNumber'],'')::bigint                    AS employee_number,
          d.value['id']::bigint                                           AS employee_id,
          d.value['customNationality']::varchar                           AS nationality,
          d.value['customRegion']::varchar                                AS region,
          d.value['ethnicity']::varchar                                   AS ethnicity,
          source.uploaded_at                                              AS uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext['employees']), outer => true) d
), final AS ( 
  
    SELECT 
            ROW_NUMBER() OVER 
                (PARTITION BY employee_number, employee_id, nationality, region, ethnicity
                Order BY employee_number, employee_id, nationality, region, ethnicity)                          AS rn, 
            employee_number,
            employee_id, 
            nationality, 
            region, 
            ethnicity,
            min(uploaded_at) as valid_from,
            max(uploaded_at) as valid_to,
    FROM intermediate tbl 
    group by  employee_number, employee_id, nationality, region, ethnicity
)  
SELECT
    employee_id,
    nationality,
    region,
    ethnicity,
    valid_from,
    valid_to
FROM final