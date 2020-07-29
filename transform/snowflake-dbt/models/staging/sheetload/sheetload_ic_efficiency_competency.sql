WITH source AS (

	SELECT *
	FROM {{ source('sheetload_ic_efficiency_competency_source') }}

{{cleanup_certificates("'ic_efficiency_competency'",
	"Email_Address")}}
