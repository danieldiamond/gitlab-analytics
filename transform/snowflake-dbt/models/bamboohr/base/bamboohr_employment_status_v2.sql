WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'employment_status') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

        SELECT d.value AS data_by_row
        FROM source,
        LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

        SELECT
            data_by_row['id']::bigint                             AS status_id,
            data_by_row['employeeId']::bigint                     AS employee_id,
            data_by_row['date']::date                             AS effective_date,
            data_by_row['employmentStatus']::varchar              AS employment_status,   
            nullif(data_by_row['terminationTypeId']::varchar, '') AS termination_type  
      FROM intermediate

), additional AS (

        SELECT
            employee_id,
            effective_date                                                                      AS valid_from_date,
            employment_status,
            termination_type,
            lead(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date)        AS valid_to_date,
            lead(employment_status) OVER (PARTITION BY employee_id ORDER BY effective_date)     AS next_status,
            lag(employment_status) OVER (PARTITION BY employee_id ORDER BY effective_date)      AS previous_status,
            case when employment_status like '%leave%' then 'L'
                when employment_status in ('Terminated','Suspended') then 'T'
                when employment_status ='Active' then 'A'
                else 'Other' end                                                                AS emp_status      
            -- will need to work with people group to create a translation table for different employment statuses
  FROM renamed r

)  

    SELECT
        employee_id,
        employment_status,
        emp_status, -- will need to work with people group to create a translation table for different employment statuses
        termination_type,
        case when previous_status ='Terminated' then 1 else 0 end                               AS is_rehire,
        next_status,           
        valid_from_date,
        ifnull(valid_to_date, last_day(current_date))                                           AS valid_to_date
    FROM additional
 