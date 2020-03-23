WITH saas_charges AS (
  
    SELECT *
    FROM {{ ref('customers_db_charges_xf') }}
    /* Manual Exclusions */
    WHERE subscription_name_slugify NOT IN (
    'accenture---intient-200-silver---initial-investment-01-20' --https://gitlab.com/gitlab-data/analytics/issues/3659
    )
    AND delivery = 'SaaS'
  
)

, namespaces AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

)

, saas_joined AS (
  
    SELECT 
      DATE_TRUNC('month', subscription_start_date::DATE) AS subscription_month, 
      subscription_name_slugify,
      delivery,
      CASE 
       WHEN SUM(month_interval) <= 12
        THEN SUM(mrr * month_interval)
       ELSE SUM(mrr * month_interval) * 12/ SUM(month_interval)
      END AS arr
    FROM saas_charges
    INNER JOIN namespaces ON saas_charges.current_gitlab_namespace_id = namespaces.namespace_id
    WHERE oldest_subscription_in_cohort = subscription_name_slugify
      AND DATE_TRUNC('month', subscription_start_date) = DATE_TRUNC('month', effective_start_date)
      AND subscription_start_date >= DATEADD(month, -12, current_date)
      AND DATEDIFF('days', TO_DATE(namespaces.namespace_created_at), subscription_start_date) <= 1
      AND DATEDIFF('days', TO_DATE(namespaces.namespace_created_at), subscription_start_date) >= 0
      AND DATE_TRUNC('month', subscription_start_date) < DATE_TRUNC('month', current_date)
    GROUP BY 1,2,3 

)


SELECT *
FROM saas_joined
