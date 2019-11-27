-- valid_from should always be less than valid_to

WITH data AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}

)

SELECT *
FROM data
WHERE valid_from > valid_to
