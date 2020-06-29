WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'ic_iteration_competency') }}

{{cleanup_certificates("'ic_iteration_competency'",
	"Email_Address")}}
