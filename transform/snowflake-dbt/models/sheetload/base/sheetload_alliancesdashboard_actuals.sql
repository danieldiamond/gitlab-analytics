WITH source AS (

        SELECT md5(month_of :: varchar)                         as pk,
               month_of :: date									as month_of,
               nullif(active_gitlab_installations, '') :: float as active_gitlab_installations,
               nullif(active_users_aws, '') :: float            as active_users_aws,
               nullif(active_users_gcp, '') :: float            as active_users_gcp,
               nullif(active_users_azure, '') :: float          as active_users_azure
        FROM raw.sheetload.alliancesdashboard_actuals
  )

SELECT *
FROM source
