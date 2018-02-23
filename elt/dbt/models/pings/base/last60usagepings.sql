SELECT
     coalesce(hostname, source_ip) AS domain,
     max(id)                       AS id,
     max(updated_at)               AS updated_at,
     max(version)                  AS gitlab_version,
     max(host_id)                  AS host_id,
     max(cast(stats AS TEXT))      AS stats,
     max(active_user_count)        AS active_user_count,
     count(*)                      AS usage_pings
   FROM version.usage_data
   WHERE version !~ '.*-ee'
         AND updated_at :: DATE >= (now() - '60 days' :: INTERVAL)
   GROUP BY domain