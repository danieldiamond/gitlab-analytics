WITH sequence_gen AS (
    -- https://docs.snowflake.net/manuals/sql-reference/functions/seq1.html#seq1-seq2-seq4-seq8
    SELECT DATEADD(day, SEQ4(), '1970-01-01' :: DATE) AS datum
    -- https://docs.snowflake.net/manuals/sql-reference/functions/generator.html
    FROM TABLE(GENERATOR(ROWCOUNT => 29219))
)

-- https://docs.snowflake.net/manuals/sql-reference/functions-date-time.html#supported-date-and-time-parts
SELECT
  TO_CHAR(datum, 'yyyymmdd') :: INT                                   AS date_dim_id,
  datum                                                               AS date_actual,
  EXTRACT(epoch_second FROM datum)                                    AS epoch,
  CASE
  WHEN TO_CHAR(datum, 'DD') = '01'
    THEN 'st'
  WHEN TO_CHAR(datum, 'DD') = '02'
    THEN 'nd'
  WHEN TO_CHAR(datum, 'DD') = '03'
    THEN 'rd'
  ELSE 'th'
  END                                                                 AS day_suffix,
  -- https://docs.snowflake.net/manuals/sql-reference/functions/dayname.html#dayname
  DAYNAME(datum)                                                      AS day_name,
  EXTRACT(dayofweek FROM datum) + 1                                   AS day_of_week,
  EXTRACT(dayofweekiso FROM datum)                                    AS day_of_week_iso,
  EXTRACT(day FROM datum)                                             AS day_of_month,
  -- https://docs.snowflake.net/manuals/sql-reference/functions/datediff.html#datediff
  DATEDIFF(day, DATE_TRUNC('quarter', datum), datum) + 1              AS day_of_quarter,
  EXTRACT(dayofyear FROM datum)                                       AS day_of_year,
  DATEDIFF(week, DATE_TRUNC('month', datum), datum) + 1               AS week_of_month,
  EXTRACT(week FROM datum)                                            AS week_of_year,
  EXTRACT(weekiso FROM datum)                                         AS week_of_year_iso,
  EXTRACT(month FROM datum)                                           AS month_actual,
  CASE
  -- https://docs.snowflake.net/manuals/sql-reference/functions/monthname.html#monthname
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
  END                                                                 AS month_name,
  MONTHNAME(datum)                                                    AS month_name_abbreviated,
  EXTRACT(quarter FROM datum)                                         AS quarter_actual,
  CASE
  WHEN EXTRACT(quarter FROM datum) = 1
    THEN 'First'
  WHEN EXTRACT(quarter FROM datum) = 2
    THEN 'Second'
  WHEN EXTRACT(quarter FROM datum) = 3
    THEN 'Third'
  WHEN EXTRACT(quarter FROM datum) = 4
    THEN 'Fourth'
  END                                                                 AS quarter_name,
  EXTRACT(yearofweek FROM datum)                                      AS year_actual,
  EXTRACT(yearofweekiso FROM datum)                                   AS year_actual_iso,
  -- https://docs.snowflake.net/manuals/sql-reference/functions/dateadd.html#dateadd
  DATEADD(day, 1 - EXTRACT(dayofweekiso FROM datum), datum)           AS first_day_of_week,
  DATEADD(day, 7 - EXTRACT(dayofweekiso FROM datum), datum)           AS last_day_of_week,
  DATEADD(day, 1 - EXTRACT(day FROM datum), datum)                    AS first_day_of_month,
  DATEADD(month, 1, DATEADD(day, -1, DATE_TRUNC('month', datum)))     AS last_day_of_month,
  DATE_TRUNC('quarter', datum) :: DATE                                AS first_day_of_quarter,
  DATEADD(quarter, 1, DATEADD(day, -1, DATE_TRUNC('quarter', datum))) AS last_day_of_quarter,
  (EXTRACT(yearofweekiso FROM datum) || '-01-01') :: DATE             AS first_day_of_year,
  (EXTRACT(yearofweekiso FROM datum) || '-12-31') :: DATE             AS last_day_of_year,
  TO_CHAR(datum, 'mmyyyy')                                            AS mmyyyy,
  TO_CHAR(datum, 'mmddyyyy')                                          AS mmddyyyy,
  CASE
  WHEN EXTRACT(dayofweekiso FROM datum) IN (6, 7)
    THEN TRUE
  ELSE FALSE
  END                                                                 AS weekend_indr
FROM sequence_gen
