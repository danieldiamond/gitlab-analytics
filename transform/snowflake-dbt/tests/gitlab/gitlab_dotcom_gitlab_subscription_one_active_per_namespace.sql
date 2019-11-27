-- Each namespace_id should only have a maximum of one currently valid row.

WITH data AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}

)

SELECT namespace_id
FROM data
WHERE is_currently_valid = True
GROUP BY namespace_id
HAVING COUNT(*) > 1