WITH unioned_list as (
    SELECT
        SAAS_FREE_USERS.*,
        'SaaS Free User' AS bucket
    FROM FREE_GITLAB_COM__20200617 as SAAS_FREE_USERS  -- replace with saas_free_users table

    UNION

    SELECT
        SAAS_PAID_USERS.*,
        'SaaS Paid User' AS bucket
    FROM PAID_GITLAB_COM__20200617 as SAAS_PAID_USERS      -- replace with saas_paid_users table
  
    LEFT JOIN FREE_GITLAB_COM__20200617 as SAAS_FREE_USERS -- replace with saas_free_users table
        ON SAAS_FREE_USERS.notification_email = SAAS_PAID_USERS.notification_email
        AND SAAS_FREE_USERS.full_name = SAAS_PAID_USERS.full_name
  
    WHERE SAAS_FREE_USERS.NOTIFICATION_EMAIL IS NULL

    UNION

    SELECT
        SELF_MANAGED_PAID_USERS.*,
        'Self-Managed' AS bucket
    FROM SELF_MANAGED_GITLAB_COM__20200617 as SELF_MANAGED_PAID_USERS -- replace with self_managed_paid 
  
    LEFT JOIN PAID_GITLAB_COM__20200617 as SAAS_PAID_USERS     -- replace with saas_paid_users table
        ON SAAS_PAID_USERS.NOTIFICATION_EMAIL = SELF_MANAGED_PAID_USERS.NOTIFICATION_EMAIL
        AND SAAS_PAID_USERS.full_name         = SELF_MANAGED_PAID_USERS.full_name
  
    LEFT JOIN FREE_GITLAB_COM__20200617 as SAAS_FREE_USERS    -- replace with saas_free_users table
        ON SAAS_FREE_USERS.NOTIFICATION_EMAIL = SELF_MANAGED_PAID_USERS.NOTIFICATION_EMAIL
        AND SAAS_FREE_USERS.full_name         = SELF_MANAGED_PAID_USERS.full_name
  
    WHERE SAAS_FREE_USERS.NOTIFICATION_EMAIL IS NULL
        AND SAAS_PAID_USERS.NOTIFICATION_EMAIL IS NULL

), unioned_list_no_dup_state AS (
    
   SELECT  DISTINCT
      user_id::INTEGER AS user_id,
      full_name,
      first_name,
      last_name,
      notification_email,
      plan_title,
      state,
      bucket
   FROM unioned_list
   QUALIFY ROW_NUMBER() OVER(PARTITION BY full_name, notification_email, plan_title ORDER BY state) = 1 -- Given a combination of email and email, if there are multiple states, only pick where active, if not inactive, if not it's only blocked

)

SELECT *
FROM unioned_list_no_dup_state
