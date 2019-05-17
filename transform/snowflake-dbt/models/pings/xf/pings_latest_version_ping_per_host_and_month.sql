{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH pings_version_checks AS (

	SELECT * FROM {{ ref('pings_version_checks') }}
),

ranked AS (
  SELECT
    id AS ping_id,
    host_id,
    created_at,
    updated_at,
    gitlab_version,
    referer_url,
    request_data,
    CASE WHEN gitlab_version LIKE '%ee%' THEN 'EE'
      ELSE 'CE' END  AS main_edition,
    ROW_NUMBER() OVER (
      PARTITION BY
        host_id,
        DATE_TRUNC('month', created_at)
      ORDER BY created_at DESC
    ) AS row_number
  FROM pings_version_checks
)

SELECT
  ping_id,
  host_id,
  created_at,
  updated_at,
  gitlab_version,
  referer_url,
  request_data,
  main_edition
FROM ranked
WHERE row_number = 1
