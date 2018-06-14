WITH max_date AS (
    SELECT
      opportunityid,
      max(systemmodstamp) AS max_time
    FROM sfdc.opportunityhistory
    GROUP BY opportunityid
),

    max_amount AS (
    /* This ensures we're getting the amount from the last action on the opp*/
      SELECT
        max(oh.amount) AS amount,
        oh.opportunityid
      FROM max_date d
        JOIN sfdc.opportunityhistory oh ON d.opportunityid = oh.opportunityid
                                           AND d.max_time = oh.systemmodstamp
      GROUP BY oh.opportunityid
  )

SELECT
  oh.*,
  (date_part('day', oh.systemmodstamp - oh.createddate) * 24 +
   date_part('hour', oh.systemmodstamp - oh.createddate)) / 24.0 AS days_in_stage,
  CASE WHEN
    ma.amount :: DECIMAL < 5000
    THEN '1 - Small (<5k)'
  WHEN ma.amount :: DECIMAL >= 5000 AND ma.amount :: DECIMAL < 25000
    THEN '2 - Medium (5k - 25k)'
  WHEN ma.amount :: DECIMAL >= 25000 AND ma.amount :: DECIMAL < 100000
    THEN '3 - Big (25k - 100k)'
  WHEN ma.amount :: DECIMAL >= 100000
    THEN '4 - Jumbo (>100k)'
  ELSE '5 -Unknown' END                                          AS deal_size
FROM sfdc.opportunityhistory oh
  JOIN max_amount ma ON oh.opportunityid = ma.opportunityid