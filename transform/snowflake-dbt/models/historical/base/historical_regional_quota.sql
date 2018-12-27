WITH source AS (

	SELECT *
	FROM raw.historical.transposed

), renamed AS (

SELECT 
  	"Bookings_at_Plan"::date 	as month_of,
  	"EMEA_"                   	as emea,
  	"Public_Sector" 			as public_sector,
	"US_East" 					as us_east,
	"US_Central" 				as us_central,
	"US_West" 					as us_west,
	"APAC" 						as apac,
	"Channel" 					as channel,
	"Self_Serve_SMB" 			as self_serve
	FROM source

)

SELECT *
FROM renamed
