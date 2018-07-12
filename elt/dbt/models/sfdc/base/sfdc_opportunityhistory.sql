WITH base AS (
    SELECT
      *,
      lead(createddate)
      OVER (
        PARTITION BY opportunityid
        ORDER BY createddate ) - opportunityhistory.createddate AS time_in_stage
    FROM sfdc.opportunityhistory
)

SELECT
  *,
  coalesce(extract(EPOCH FROM time_in_stage) / (3600 * 24), 0) AS days_in_stage
FROM base