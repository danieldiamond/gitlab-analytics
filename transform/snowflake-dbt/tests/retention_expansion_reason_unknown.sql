-- Fail if there are Unkown reasons for retention, positive or negative.

SELECT *
FROM {{ref('retention_reasons_for_retention')}}
WHERE churn_type = 'Unknown'