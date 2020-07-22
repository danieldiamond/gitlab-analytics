WITH dates AS (
    SELECT *
    FROM {{ ref('date_details') }}
)

SELECT
  TO_NUMBER(TO_CHAR(date_actual,'YYYYMMDD'),'99999999') AS date_id,
  *
FROM dates