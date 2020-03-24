WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'employment_status') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate as (

      SELECT d.value AS data_by_row
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed as (

      SELECT
            data_by_row['id']::BIGINT                             AS status_id,
            data_by_row['employeeId']::BIGINT                     AS employee_id,
            data_by_row['date']::DATE                             AS effective_date,
            data_by_row['employmentStatus']::VARCHAR              AS employment_status, 
            nullif(data_by_row['terminationTypeId']::VARCHAR, '') AS termination_type
      FROM intermediate

)

SELECT *
FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id,effective_date,employment_status ORDER BY effective_date) = 1
