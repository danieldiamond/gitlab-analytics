WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'custom_bonus') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

      SELECT d.value as data_by_row
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), unnest_again AS (

      SELECT d.value as data_by_row
      FROM intermediate,
      LATERAL FLATTEN(INPUT => parse_json(data_by_row), outer => true) d

), renamed AS (

      SELECT
           data_by_row['id']::bigint               AS bonus_id,
           data_by_row['employeeId']::bigint       AS employee_id,
           data_by_row['customBonustype']::varchar AS bonus_type,
           data_by_row['customBonusdate']::date    AS bonus_date,
           data_by_row['customNominatedBy']::varchar AS bonus_nominator_type
      FROM unnest_again

)

SELECT *
FROM renamed
