WITH base_date as (
  SELECT * FROM {{ ref('dim_date') }}
),

the_dates AS (
    SELECT
      date_actual,
      day_of_quarter
    FROM base_date
    WHERE
      date_actual >= '2017-07-01'
      AND date_actual <= '2024-12-31'
),

won_opps AS (
    SELECT
      sum(iacv) AS iacv,
      a.opportunity_closedate
    FROM analytics.f_opportunity a
      JOIN analytics.dim_opportunitystage s ON a.opportunity_stage_id = s.id
    WHERE s.iswon = TRUE

    GROUP BY opportunity_closedate
)


SELECT
  d.date_actual,
  d.day_of_quarter,
  CASE
    WHEN o.iacv IS NULL THEN 0
    ELSE o.iacv END as iacv
FROM the_dates d
  FULL JOIN won_opps o ON o.opportunity_closedate = d.date_actual
WHERE d.date_actual IS NOT NULL
ORDER BY d.date_actual
DESC