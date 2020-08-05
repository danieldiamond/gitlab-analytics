WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'google_referrals') }}

), renamed AS (


    SELECT
      date_week::DATE              AS date_week,
      eligible_upgrades::NUMBER    AS eligible_upgrades,
      trials_referred::NUMBER      AS trials_referred,
      upgrade_revenue::NUMBER      AS upgrade_revenue,
      trial_revenue::NUMBER        AS trial_revenue
    FROM source

)

SELECT *
FROM renamed
