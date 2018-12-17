WITH source AS (

	SELECT *
	FROM raw.historical.google_referrals

), renamed AS (


	SELECT
       date_week::DATE,
		   eligible_upgrades::INTEGER,
       trials_referred::INTEGER,
       upgrade_revenue::INTEGER,
       trial_revenue::INTEGER
	FROM source

)

SELECT *
FROM renamed
