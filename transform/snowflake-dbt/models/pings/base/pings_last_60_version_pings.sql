SELECT *
FROM {{ var("database") }}.gcloud_postgres_stitch.version_version_checks
WHERE updated_at :: DATE >= dateadd(day, -60, CURRENT_DATE)
     AND gitlab_version NOT LIKE '%ee'
     AND gitlab_version NOT LIKE '%pre'