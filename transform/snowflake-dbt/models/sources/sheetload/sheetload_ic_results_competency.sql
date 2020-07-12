WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'ic_results_competency') }}

{{cleanup_certificates("'ic_results_competency'",
	"Email_Address")}}
