WITH sequence_gen AS (
    -- https://docs.snowflake.net/manuals/sql-reference/functions/seq1.html#seq1-seq2-seq4-seq8
    SELECT DATEADD(day, SEQ4(), '1970-01-01' :: DATE)       AS datum
    -- https://docs.snowflake.net/manuals/sql-reference/functions/generator.html
    FROM TABLE(GENERATOR(ROWCOUNT => 29219))
)

-- https://docs.snowflake.net/manuals/sql-reference/functions-date-time.html#supported-date-and-time-parts
SELECT
  TO_CHAR(datum, 'yyyymmdd') :: INT                         AS date_dim_id,
  datum                                                     AS date_actual,
  EXTRACT(epoch_second FROM datum)                          AS epoch,
  CASE
  WHEN MOD(TO_CHAR(datum, 'dd') :: INT, 10) = 1
    THEN TO_CHAR(datum, 'dd') :: INT || 'st'
  WHEN MOD(TO_CHAR(datum, 'dd') :: INT, 10) = 2
    THEN TO_CHAR(datum, 'dd') :: INT || 'nd'
  WHEN MOD(TO_CHAR(datum, 'dd') :: INT, 10) = 3
    THEN TO_CHAR(datum, 'dd') :: INT || 'rd'
  ELSE TO_CHAR(datum, 'dd') :: INT || 'th'
  END                                                       AS day_suffix,
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
  END                                                       AS day_name,
  EXTRACT(dayofweek FROM datum) + 1                         AS day_of_week,
  EXTRACT(dayofweekiso FROM datum)                          AS day_of_week_iso,
  EXTRACT(day FROM datum)                                   AS day_of_month,
  -- https://docs.snowflake.net/manuals/sql-reference/functions/datediff.html#datediff
  DATEDIFF(day, DATE_TRUNC('quarter', datum), datum) + 1    AS day_of_quarter,
  EXTRACT(dayofyear FROM datum)                             AS day_of_year,
  DATEDIFF(week, DATE_TRUNC('month', datum), datum) + 1     AS week_of_month,
  EXTRACT(week FROM datum)                                  AS week_of_year,
  EXTRACT(weekiso FROM datum)                               AS week_of_year_iso,
  EXTRACT(month FROM datum)                                 AS month_actual,
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
  END                                                       AS month_name,
  MONTHNAME(datum)                                          AS month_name_abbreviated,
  EXTRACT(quarter FROM datum)                               AS quarter_actual,
  CASE
  WHEN EXTRACT(quarter FROM datum) = 1
    THEN 'First'
  WHEN EXTRACT(quarter FROM datum) = 2
    THEN 'Second'
  WHEN EXTRACT(quarter FROM datum) = 3
    THEN 'Third'
  WHEN EXTRACT(quarter FROM datum) = 4
    THEN 'Fourth'
  END                                                       AS quarter_name,
  EXTRACT(yearofweek FROM datum)                            AS year_actual,
  EXTRACT(yearofweekiso FROM datum)                         AS year_actual_iso,
  -- https://docs.snowflake.net/manuals/sql-reference/functions/dateadd.html#dateadd
  DATEADD(day, 1 - EXTRACT(dayofweekiso FROM datum), datum) AS first_day_of_week,
  DATEADD(day, 7 - EXTRACT(dayofweekiso FROM datum), datum) AS last_day_of_week,
  -- https://docs.snowflake.net/manuals/sql-reference/functions/last_day.html#last-day
  DATE_TRUNC('month', datum)                                AS first_day_of_month,
  LAST_DAY(datum, 'month')                                  AS last_day_of_month,
  DATE_TRUNC('quarter', datum)                              AS first_day_of_quarter,
  LAST_DAY(datum, 'quarter')                                AS last_day_of_quarter,
  (EXTRACT(yearofweekiso FROM datum) || '-01-01') :: DATE   AS first_day_of_year,
  (EXTRACT(yearofweekiso FROM datum) || '-12-31') :: DATE   AS last_day_of_year,
  TO_CHAR(datum, 'mmyyyy')                                  AS mmyyyy,
  TO_CHAR(datum, 'mmddyyyy')                                AS mmddyyyy,
  CASE
  WHEN EXTRACT(dayofweekiso FROM datum) IN (6, 7)
    THEN TRUE
  ELSE FALSE
  END                                                       AS weekend_indr
FROM sequence_gen
