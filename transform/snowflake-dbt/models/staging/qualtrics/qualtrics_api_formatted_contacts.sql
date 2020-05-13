{{ config({
        "schema": "sensitive"
    })
}}

WITH users AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users') }}

), renamed AS (

    SELECT
      user_id                                                AS user_id,
      IFNULL(first_name, SPLIT_PART(users_name, ' ', 1))     AS first_name,
      LTRIM(IFNULL(last_name,  LTRIM(users_name, SPLIT_PART(users_name, ' ', 1))))     AS last_name,
      IFNULL(notification_email, public_email)               AS email_address,
      NULL                                                   AS phone_number,
      UPPER(IFNULL(NULLIF(preferred_language, 'nan'), 'en')) AS language
    FROM users

)

SELECT *
FROM renamed