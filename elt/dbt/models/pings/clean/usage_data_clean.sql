with version60 as (
  SELECT * FROM {{ ref('last60usagepings') }}
)

SELECT
  curls.clean_domain  AS clean_url,
  coalesce(version60.hostname, version60.source_ip) AS raw_domain,
  version60.*
FROM
  version60
  JOIN cleaned_urls AS curls ON coalesce(version60.hostname, version60.source_ip) = curls.domain