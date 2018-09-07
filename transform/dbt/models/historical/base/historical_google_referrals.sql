WITH source AS (

	SELECT *
	FROM historical.google_referrals

), renamed AS (


	SELECT
       date_week,
		   eligible_upgrades,
       trials_referred,
       upgrade_revenue,
       trial_revenue
	FROM source

)

SELECT *
FROM renamed