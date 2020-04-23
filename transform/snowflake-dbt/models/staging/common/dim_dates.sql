WITH dates AS (
    SELECT *
    FROM {{ ref('date_details') }}
)

SELECT
  to_number(to_char(date_actual,'YYYYMMDD'),'99999999') as date_id,
  *
FROM dates