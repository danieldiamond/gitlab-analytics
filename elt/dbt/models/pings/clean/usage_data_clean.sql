with version60 as (
  SELECT * FROM {{ ref('last60usagepings') }}
)

SELECT
  curls.clean_domain  AS clean_url,
  version60.domain       AS raw_domain,
  id                  AS id,
  updated_at          AS updated_at,
  gitlab_version      AS gitlab_version,
  host_id             AS host_id,
  cast(stats AS JSON) AS stats,
  active_user_count   AS active_user_count,
  usage_pings         AS usage_pings
FROM
  version60
  JOIN cleaned_urls AS curls ON version60.domain = curls.domain