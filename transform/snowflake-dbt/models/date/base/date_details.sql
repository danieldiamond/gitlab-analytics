{{ config(schema='analytics') }}

WITH sequence_gen AS (
  -- https://docs.snowflake.net/manuals/sql-reference/functions/seq1.html#seq1-seq2-seq4-seq8
    SELECT
      DATEADD(day, SEQ4(), '1970-01-01' :: DATE)                      AS datum,
      DATEADD(day, SEQ4(), '1970-01-01' :: DATE) - INTERVAL '1 month' AS fy_datum
    -- https://docs.snowflake.net/manuals/sql-reference/functions/generator.html
    FROM TABLE(GENERATOR(ROWCOUNT => 29219))
)

-- https://docs.snowflake.net/manuals/sql-reference/functions-date-time.html#supported-date-and-time-parts
SELECT
  TO_CHAR(datum, 'yyyymmdd') :: INT                              AS date_dim_id,
  datum                                                          AS date_actual,
  EXTRACT(epoch_second FROM datum)                               AS epoch,
  CASE
  WHEN MOD(TO_CHAR(datum, 'dd') :: INT, 10) = 1
    THEN TO_CHAR(datum, 'dd') :: INT || 'st'
  WHEN MOD(TO_CHAR(datum, 'dd') :: INT, 10) = 2
    THEN TO_CHAR(datum, 'dd') :: INT || 'nd'
  WHEN MOD(TO_CHAR(datum, 'dd') :: INT, 10) = 3
    THEN TO_CHAR(datum, 'dd') :: INT || 'rd'
  ELSE TO_CHAR(datum, 'dd') :: INT || 'th'
  END                                                            AS day_suffix,
  -- https://docs.snowflake.net/manuals/sql-reference/functions/dayname.html#dayname
  CASE
  WHEN DAYNAME(datum) = 'Mon'
    THEN 'Monday'
  WHEN DAYNAME(datum) = 'Tue'
    THEN 'Tuesday'
  WHEN DAYNAME(datum) = 'Wed'
    THEN 'Wednesday'
  WHEN DAYNAME(datum) = 'Thu'
    THEN 'Thursday'
  WHEN DAYNAME(datum) = 'Fri'
    THEN 'Friday'
  WHEN DAYNAME(datum) = 'Sat'
    THEN 'Saturday'
  WHEN DAYNAME(datum) = 'Sun'
    THEN 'Sunday'
  END                                                            AS day_name,
  EXTRACT(dayofweek FROM datum) + 1                              AS day_of_week,
  EXTRACT(dayofweekiso FROM datum)                               AS day_of_week_iso,
  EXTRACT(DAY FROM datum)                                        AS day_of_month,

  -- https://docs.snowflake.net/manuals/sql-reference/functions/datediff.html#datediff
  DATEDIFF(day, DATE_TRUNC('quarter', datum), datum) + 1         AS day_of_quarter,
  EXTRACT(dayofyear FROM datum)                                  AS day_of_year,
  DATEDIFF(day, DATE_TRUNC('quarter', fy_datum), fy_datum) + 1   AS fy_day_of_quarter,
  EXTRACT(dayofyear FROM fy_datum)                               AS fy_day_of_year,

  DATEDIFF(week, DATE_TRUNC('month', datum), datum) + 1          AS week_of_month,

  EXTRACT(WEEK FROM datum)                                       AS week_of_year,
  EXTRACT(WEEK FROM fy_datum)                                    AS fy_week_of_year,
  -- snowflake WEEKISO doesn't properly return guaranteed two digit weeks
  YEAROFWEEKISO(datum)
  || CASE
     WHEN LENGTH(WEEKISO(datum)) = 1
       THEN CONCAT('-W0', WEEKISO(datum))
     ELSE CONCAT('-W', WEEKISO(datum))
     END
  || CONCAT('-', DAYOFWEEKISO(datum))                            AS week_of_year_iso,
  EXTRACT(MONTH FROM datum)                                      AS month_actual,

  YEAROFWEEKISO(fy_datum) + 1
  || CASE
     WHEN LENGTH(WEEKISO(fy_datum)) = 1
       THEN CONCAT('-W0', WEEKISO(fy_datum))
     ELSE CONCAT('-W', WEEKISO(fy_datum))
     END
  || CONCAT('-', DAYOFWEEKISO(fy_datum))                         AS fy_week_of_year_iso,
  EXTRACT(MONTH FROM fy_datum)                                   AS fy_month_actual,
  -- https://docs.snowflake.net/manuals/sql-reference/functions/monthname.html#monthname
  CASE
  WHEN MONTHNAME(datum) = 'Jan'
    THEN 'January'
  WHEN MONTHNAME(datum) = 'Feb'
    THEN 'February'
  WHEN MONTHNAME(datum) = 'Mar'
    THEN 'March'
  WHEN MONTHNAME(datum) = 'Apr'
    THEN 'April'
  WHEN MONTHNAME(datum) = 'May'
    THEN 'May'
  WHEN MONTHNAME(datum) = 'Jun'
    THEN 'June'
  WHEN MONTHNAME(datum) = 'Jul'
    THEN 'July'
  WHEN MONTHNAME(datum) = 'Aug'
    THEN 'August'
  WHEN MONTHNAME(datum) = 'Sep'
    THEN 'September'
  WHEN MONTHNAME(datum) = 'Oct'
    THEN 'October'
  WHEN MONTHNAME(datum) = 'Nov'
    THEN 'November'
  WHEN MONTHNAME(datum) = 'Dec'
    THEN 'December'
  END                                                            AS month_name,
  MONTHNAME(datum)                                               AS month_name_abbreviated,
  EXTRACT(QUARTER FROM datum)                                    AS quarter_actual,
  EXTRACT(QUARTER FROM fy_datum)                                 AS fy_quarter_actual,
  CASE
  WHEN EXTRACT(QUARTER FROM datum) = 1
    THEN 'First'
  WHEN EXTRACT(QUARTER FROM datum) = 2
    THEN 'Second'
  WHEN EXTRACT(QUARTER FROM datum) = 3
    THEN 'Third'
  WHEN EXTRACT(QUARTER FROM datum) = 4
    THEN 'Fourth'
  END                                                            AS quarter_name,

  CASE
  WHEN EXTRACT(QUARTER FROM fy_datum) = 1
    THEN 'First'
  WHEN EXTRACT(QUARTER FROM fy_datum) = 2
    THEN 'Second'
  WHEN EXTRACT(QUARTER FROM fy_datum) = 3
    THEN 'Third'
  WHEN EXTRACT(QUARTER FROM fy_datum) = 4
    THEN 'Fourth'
  END                                                            AS fy_quarter_name,

  EXTRACT(yearofweek FROM datum)                                 AS year_actual,
  EXTRACT(yearofweekiso FROM datum)                              AS year_actual_iso,

  EXTRACT(yearofweek FROM fy_datum) + 1                          AS fy_year_actual,
  EXTRACT(yearofweekiso FROM fy_datum) + 1                       AS fy_year_actual_iso,

  -- https://docs.snowflake.net/manuals/sql-reference/functions/dateadd.html#dateadd
  DATEADD(day, 1 - EXTRACT(dayofweekiso FROM datum), datum)      AS first_day_of_week,
  DATEADD(day, 7 - EXTRACT(dayofweekiso FROM datum), datum)      AS last_day_of_week,
  -- https://docs.snowflake.net/manuals/sql-reference/functions/last_day.html#last-day
  DATE_TRUNC('month', datum)                                     AS first_day_of_month,
  LAST_DAY(datum, 'month')                                       AS last_day_of_month,

  DATE_TRUNC('quarter', datum)                                   AS first_day_of_quarter,
  LAST_DAY(datum, 'quarter')                                     AS last_day_of_quarter,
  DATE_TRUNC('quarter', fy_datum) + INTERVAL '1 month'           AS fy_first_day_of_quarter,
  LAST_DAY(fy_datum, 'quarter') + INTERVAL '1 month'             AS fy_last_day_of_quarter,

  (EXTRACT(yearofweekiso FROM datum) || '-01-01') :: DATE        AS first_day_of_year,
  (EXTRACT(yearofweekiso FROM datum) || '-12-31') :: DATE        AS last_day_of_year,
  (EXTRACT(yearofweekiso FROM fy_datum) + 1 || '-02-01') :: DATE AS fy_first_day_of_year,
  (EXTRACT(yearofweekiso FROM fy_datum) + 1 || '-01-31') :: DATE AS fy_last_day_of_year,

  TO_CHAR(datum, 'mmyyyy')                                       AS mmyyyy,
  TO_CHAR(datum, 'mmddyyyy')                                     AS mmddyyyy,
  CASE
  WHEN EXTRACT(dayofweekiso FROM datum) IN (6, 7)
    THEN TRUE
  ELSE FALSE
  END                                                            AS weekend_indr
FROM sequence_gen