WITH users AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom', 'gitlab_dotcom_users') }}

), renamed AS (

    SELECT
      user_id                                                AS user_id,
      IFNULL(first_name, SPLIT_PART(users_name, ' ', 1))     AS first_name,
      IFNULL(last_name,  LTRIM(users_name, SPLIT_PART(users_name, ' ', 1) || ' '))     AS last_name,
      IFNULL(notification_email, public_email)               AS email_address,
      NULL                                                   AS phone_number,
      IFNULL(NULLIF(preferred_language, 'nan'), 'EN')        AS language
    FROM users

)

SELECT *
FROM renamed