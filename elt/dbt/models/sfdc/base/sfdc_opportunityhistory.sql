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
     date_part('hour', oh.systemmodstamp - oh.createddate)) / 24.0 AS days_in_stage
FROM sfdc.opportunityhistory oh
  JOIN max_amount ma ON oh.opportunityid = ma.opportunityid