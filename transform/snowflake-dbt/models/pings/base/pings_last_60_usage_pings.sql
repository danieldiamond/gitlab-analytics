SELECT
 *
FROM {{ var("database") }}.gcloud_postgres_stitch.version_usage_data
WHERE updated_at::DATE >= dateadd(day, -60, CURRENT_DATE)
     AND version NOT LIKE '%ee'
     AND version NOT LIKE '%pre'

