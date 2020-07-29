WITH source AS (

	SELECT *
	FROM {{ source('sheetload_ic_results_competency_source') }}

{{cleanup_certificates("'ic_results_competency'",
	"Email_Address")}}
