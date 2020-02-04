{% set repeated_column_names = "gender, ethnicity, nationality, region," %}

with dates AS (

    SELECT
      date_actual                                 AS start_date,
      LAST_DAY(date_actual)                       AS end_date
    FROM {{ ref ('date_details') }}
    WHERE date_day <= LAST_DAY(current_date)
       AND day_of_month = 1
       AND date_actual >= '2013-07-01' -- min employment_status_date in bamboohr_employment_status model

), mapping AS (

  SELECT *
  FROM {{ref('bamboohr_id_employee_number_mapping')}}

), bamboohr_employment_status_xf as (

  SELECT *
  FROM {{ref('bamboohr_employment_status_xf')}}

), employees AS (

    SELECT
      bamboohr_employment_status_xf.*,
      COALESCE(mapping.gender,'unidentified')             AS gender,
      COALESCE(mapping.ethnicity, 'unidentified')         AS ethnicity,
      COALESCE(mapping.nationality, 'unidentified')       AS nationality,
      COALESCE(mapping.region, 'unidentified')            AS region,
      ROW_NUMBER() OVER (PARTITION BY bamboohr_employment_status_xf.employee_id ORDER BY valid_from_date) AS employee_status_event
    FROM bamboohr_employment_status_xf
    LEFT JOIN mapping
       ON bamboohr_employment_status_xf.employee_id = mapping.employee_id

),headcount_start AS (

    SELECT
      start_date                                          AS month_date,
      {{repeated_column_names}}
      COUNT(employee_id)                                  AS headcount_start
    FROM dates
    LEFT JOIN employees
      ON dates.start_date BETWEEN valid_from_date AND valid_to_date
    GROUP BY 1,2,3,4,5

), headcount_end AS (

    SELECT
      end_date,
      {{repeated_column_names}}
      COUNT(employee_id)                                  AS headcount_end
  FROM dates
  LEFT JOIN employees
    ON dates.end_date BETWEEN valid_from_date AND valid_to_date
  GROUP BY 1,2,3,4,5

), separated AS (

    SELECT
      DATE_TRUNC('month',dates.start_date)                AS separation_month,
      {{repeated_column_names}}
      COUNT(employee_id)                                  AS total_separated,
      SUM(IFF(termination_type = 'Voluntary',1,0))        AS voluntary_separation,
      SUM(IFF(termination_type = 'Involuntary',1,0))      AS involuntary_separation
  FROM dates
  LEFT JOIN employees
    ON DATE_TRUNC('month',dates.start_date) = DATE_TRUNC('month',valid_from_date)
  WHERE employment_status='Terminated'
  GROUP BY 1,2,3,4,5

), hires AS (

    SELECT
      DATE_TRUNC('month', dates.start_date)          AS hire_month,
      {{repeated_column_names}}
      SUM(IFF(employee_status_event = 1,1,0))        AS total_hired
    FROM dates
    LEFT JOIN employees
       ON DATE_TRUNC('month',dates.start_date) = DATE_TRUNC('month',valid_from_date)
    WHERE employment_status<>'Terminated'
    GROUP BY 1,2,3,4,5

), aggregated AS (

    SELECT
      month_date,
      {{repeated_column_names}}
      headcount_start               AS total_count,
     'headcount_start'              AS metric
    FROM headcount_start

    UNION ALL

    SELECT
       headcount_end.*,
       'headcount_end'              AS metric
    FROM headcount_end

    UNION ALL

    SELECT
       hires.*,
       'hires'                      AS metric
    FROM hires

    UNION ALL

    SELECT
      separation_month,
      {{repeated_column_names}}
      total_separated,
      'total_separated'             AS metric
    FROM separated

    UNION ALL

    SELECT
      separation_month,
      {{repeated_column_names}}
      voluntary_separation,
      'voluntary_separations'       AS metric
    FROM separated

    UNION ALL

    SELECT
      separation_month,
      {{repeated_column_names}}
      involuntary_separation,
      'involuntary_separations'     AS metric
  FROM separated

 )

 SELECT *
 FROM aggregated
