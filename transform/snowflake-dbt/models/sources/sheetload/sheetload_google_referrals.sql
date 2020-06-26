WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'google_referrals') }}

), renamed AS (


	SELECT
      date_week::DATE               AS date_week,
	  eligible_upgrades::INTEGER    AS eligible_upgrades,
      trials_referred::INTEGER      AS trials_referred,
      upgrade_revenue::INTEGER      AS upgrade_revenue,
      trial_revenue::INTEGER        AS trial_revenue
	FROM source

)

SELECT *
FROM renamed
