-- Test for no overlaps on a random date (2019-09-01). Each namespace should have a maximum of one row per date.

WITH data AS (
  
    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}
  
), grouped AS (
  
    SELECT
      namespace_id,
      COUNT(*) AS count
    FROM data
      INNER JOIN (SELECT 1) AS a
        ON '2019-09-01' BETWEEN data.valid_from AND COALESCE(data.valid_to, '9999-12-31')
     GROUP BY 1
   
 )
 
SELECT *
FROM grouped
WHERE count != 1
