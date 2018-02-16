with version60 as (
  SELECT * FROM {{ ref('last60versionpings') }}
)

SELECT
  curls.clean_domain         AS clean_url,
  max(version60.referer_url) AS referer_url,
  max(gitlab_version)        AS gitlab_version,
  max(updated_at)            AS updated_at,
  max(request_data)          AS request_data,
  sum(ping_count)            AS ping_count,
  max(host_id)               AS host_id
FROM
  version60
  JOIN public.cleaned_urls AS curls ON version60.referer_url = curls.domain
GROUP BY clean_url



