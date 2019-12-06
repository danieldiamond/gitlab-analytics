WITH opphistory AS (
		SELECT * FROM {{ ref('sfdc_opportunity_history') }}
),

stages AS (
        SELECT * FROM {{ ref('sfdc_opportunity_stage') }}
),

all_stages AS (
-- Gets a mapping of opp id to all mapped stages
        SELECT
          opphistory.opportunity_id,                                                    
          stages.mapped_stage
        FROM opphistory
        CROSS JOIN stages
),

agg_days AS (
-- Gets each mapped stage in opp history and the days in that stage
       SELECT
        stages.mapped_stage,
        opphistory.opportunity_id,
        sum(days_in_stage) AS days_in_stage
       FROM opphistory
       JOIN stages
       ON opphistory.stage_name = stages.primary_label
       GROUP BY 1, 2
)

-- Gets ALL mapped stages and the appropriate value from the calculation
SELECT
    {{ dbt_utils.surrogate_key('all_stages.opportunity_id', 'all_stages.mapped_stage') }} AS primary_key,
    all_stages.opportunity_id,
    all_stages.mapped_stage,
--    Each opp has a row for each stage, but gets no credit if it was never in the stage.
    CASE
        WHEN agg_days.days_in_stage=0 THEN 0.0001
        ELSE coalesce(agg_days.days_in_stage, 0.0001) END AS days_in_stage
FROM all_stages
FULL OUTER JOIN agg_days
  ON all_stages.opportunity_id = agg_days.opportunity_id
     AND all_stages.mapped_stage = agg_days.mapped_stage
GROUP BY 1, 2, 3, 4
