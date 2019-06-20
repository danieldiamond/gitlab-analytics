with source as (

    SELECT *
    FROM {{ source('bamboohr', 'job_info') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate as (

      SELECT d.value as data_by_row
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed as (

      SELECT data_by_row['id']::bigint          AS job_id,
             data_by_row['employeeId']::bigint  AS employee_id,
             data_by_row['jobTitle']::varchar   AS job_title,
             data_by_row['date']::date          AS effective_date,
             data_by_row['department']::varchar AS department,
             data_by_row['division']::varchar   AS division,
             data_by_row['location']::varchar   AS entity,
             data_by_row['reportsTo']::varchar  AS reports_to
      FROM intermediate

)

SELECT *
FROM renamed
