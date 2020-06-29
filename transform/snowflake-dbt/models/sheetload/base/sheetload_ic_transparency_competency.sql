WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'ic_transparency_competency') }}

{{cleanup_certificates("'ic_transparency_competency'",
	"Email_Address")}}
