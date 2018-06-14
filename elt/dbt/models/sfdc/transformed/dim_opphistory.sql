WITH opphistory AS (
		SELECT * FROM {{ ref('opportunityhistory') }}
),

stages AS (
        SELECT * FROM {{ ref('mapped_stages') }}
)

SELECT
    o.*,
   s.mapped_stage
FROM opphistory o
JOIN stages s
ON o.stagename = s.masterlabel