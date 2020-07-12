WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'ic_dib_comptency') }}

{{cleanup_certificates("'ic_dib_comptency'",
	"Email_Address")}}
