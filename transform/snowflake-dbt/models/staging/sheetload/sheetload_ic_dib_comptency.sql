WITH source AS (

	SELECT *
	FROM {{ source('sheetload_ic_dib_comptency_source') }}

{{cleanup_certificates("'ic_dib_comptency'",
	"Email_Address")}}
