WITH source AS (

	SELECT *
	FROM {{ source('sheetload_ic_transparency_competency_source') }}

{{cleanup_certificates("'ic_transparency_competency'",
	"Email_Address")}}
