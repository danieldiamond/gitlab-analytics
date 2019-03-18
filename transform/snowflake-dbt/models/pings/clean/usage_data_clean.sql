with usage60 as (

  SELECT * FROM {{ ref('pings_usage_data') }}
  WHERE updated_at::DATE >= dateadd(day, -60, CURRENT_DATE)
  AND version NOT LIKE '%ee'
  AND version NOT LIKE '%pre'

)

SELECT
  curls.clean_domain                                AS clean_url,
  coalesce(usage60.hostname, usage60.source_ip) AS raw_domain,
  usage60.*
FROM
  usage60
  JOIN {{ref('cleaned_urls')}} AS curls ON coalesce(usage60.hostname, usage60.source_ip) = curls.domain

