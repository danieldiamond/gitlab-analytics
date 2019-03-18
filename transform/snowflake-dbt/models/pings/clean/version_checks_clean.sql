with version60 as (
	
  SELECT * FROM {{ ref('pings_version_checks') }}
  WHERE updated_at::DATE >= dateadd(day, -60, CURRENT_DATE)
  AND gitlab_version NOT LIKE '%ee'
  AND gitlab_version NOT LIKE '%pre'

)

SELECT
  curls.clean_domain      AS clean_url,
  curls.clean_full_domain AS clean_full_url,
  version60.*
FROM
  version60
  JOIN {{ref('cleaned_urls')}} AS curls ON version60.referer_url = curls.domain

