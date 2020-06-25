WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'compensation_certificate') }}

{{cleanup_certificates("'compensation_certificate'",
	"Email_Address")}}
