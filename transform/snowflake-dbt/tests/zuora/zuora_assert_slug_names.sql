-- Validates that the slugify function is properly maintaining unique subscription names

WITH slugs AS (

    SELECT
      count(DISTINCT subscription_name)         AS original_name,
      count(DISTINCT subscription_name_slugify) AS slug_name
    FROM {{ ref("zuora_subscription") }}

)

SELECT *
FROM slugs
WHERE original_name != slug_name