{%- macro row_count_standard_deviations(min_stddevs=5) %}
,

daily_counts AS (
  -- Round hour to closest 6-hour increment to mirror ETL design
  SELECT
    timestamp_column::DATE        AS date,
    CASE
      WHEN EXTRACT('hour', timestamp_column) < 6  THEN 6
      WHEN EXTRACT('hour', timestamp_column) < 12 THEN 12
      WHEN EXTRACT('hour', timestamp_column) < 18 THEN 18
      WHEN EXTRACT('hour', timestamp_column) < 24 THEN 24
      ELSE 0
    END                          AS rounded_hour_of_day,
    DAYOFWEEK(timestamp_column)  AS day_of_week,
    DAYOFYEAR(timestamp_column)  AS day_of_year,
    COUNT(DISTINCT unique_id)    AS count_unique_ids
  FROM data
  GROUP BY 1,2,3,4

),

windowed AS (

  SELECT
    daily_counts.*,

    AVG(count_unique_ids) OVER (
      PARTITION BY day_of_week, rounded_hour_of_day
      ORDER BY date
      ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING)   AS average_count_last_12,

    STDDEV(count_unique_ids) OVER (
      PARTITION BY day_of_week, rounded_hour_of_day
      ORDER BY date
      ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING)   AS stddev_count_last_12,

    ABS(count_unique_ids - average_count_last_12)  AS absolute_difference,
    absolute_difference / stddev_count_last_12     AS absolute_difference_stddevs 

  FROM daily_counts
  WHERE date BETWEEN '2019-01-01' AND CURRENT_DATE()

)

SELECT
  *
FROM windowed
WHERE absolute_difference_stddevs > {{min_stddevs}}
  AND count_unique_ids < average_count_last_12  -- Only concerned if LESS than the average
  AND day_of_year NOT IN (                     
    356,357,358,359,360,361,362,363,364,365,1 -- Christmas
  )
ORDER BY absolute_difference_stddevs DESC

{%- endmacro -%}