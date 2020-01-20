with source as (

    SELECT *
    FROM {{ source('bamboohr', 'employment_status') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate as (

      SELECT d.value as data_by_row
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed as (

      SELECT
            data_by_row['id']::bigint                             AS status_id,
            data_by_row['employeeId']::bigint                     AS employee_id,
            data_by_row['date']::date                             AS effective_date,
            data_by_row['employmentStatus']::varchar              AS employment_status, 
            nullif(data_by_row['terminationTypeId']::varchar, '') AS termination_type
      FROM intermediate

)

SELECT *
FROM renamed
