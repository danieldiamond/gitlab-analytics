SELECT
  referer_url,
  max(host_id)        AS host_id,
  max(gitlab_version) AS gitlab_version,
  max(updated_at)     AS updated_at,
  max(request_data)   AS request_data,
  count(*)            AS ping_count
FROM version.version_checks
WHERE updated_at :: DATE >= (now() - '60 days' :: INTERVAL)
      AND gitlab_version !~ '.*ee'
GROUP BY version_checks.referer_url