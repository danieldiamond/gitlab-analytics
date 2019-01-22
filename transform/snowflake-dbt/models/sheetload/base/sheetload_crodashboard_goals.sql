WITH source AS (

	SELECT md5(month_of :: varchar)                  as pk,
				 month_of :: date         				   as month_of,
				 nullif(ramped_reps_on_quota, '') :: float as ramped_reps_on_quota
	FROM raw.sheetload.crodashboard_goals
)

SELECT *
FROM source
