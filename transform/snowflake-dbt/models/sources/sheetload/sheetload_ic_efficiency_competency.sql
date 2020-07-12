WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'ic_efficiency_competency') }}

{{cleanup_certificates("'ic_efficiency_competency'",
	"Email_Address")}}
