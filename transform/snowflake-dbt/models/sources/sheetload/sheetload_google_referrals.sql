WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'google_referrals') }}

), renamed AS (


	SELECT
       date_week::DATE as date_week,
	     eligible_upgrades::INTEGER as eligible_upgrades,
       trials_referred::INTEGER as trials_referred,
       upgrade_revenue::INTEGER as upgrade_revenue,
       trial_revenue::INTEGER as trial_revenue
	FROM source

)

SELECT *
FROM renamed
