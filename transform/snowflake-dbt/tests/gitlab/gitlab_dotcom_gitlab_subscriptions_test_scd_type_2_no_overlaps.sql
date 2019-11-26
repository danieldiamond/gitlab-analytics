WITH grouped AS (
  SELECT
    namespace_id,
    COUNT(*) AS count
  FROM ekastelein_scratch_staging.gitlab_dotcom_gitlab_subscriptions AS data
    LEFT JOIN (SELECT 'a') AS a
      ON CURRENT_DATE BETWEEN data.valid_from AND COALESCE(data.valid_to, '9999-12-31')
   GROUP BY 1
 )
 
SELECT *
FROM grouped
WHERE count != 1