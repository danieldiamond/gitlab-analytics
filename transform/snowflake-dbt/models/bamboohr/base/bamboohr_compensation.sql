WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'compensation') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

      SELECT d.value as data_by_row
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

      SELECT
            data_by_row['id']::bigint          AS compensation_update_id,
            data_by_row['employeeId']::bigint  AS employee_id,
            data_by_row['startDate']::date     AS effective_date,
            data_by_row['type']::varchar       AS compensation_type,
            data_by_row['reason']::varchar     AS compensation_change_reason
      FROM intermediate

)

SELECT *
FROM renamed 
