WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'ic_collaboration_competency') }}

{{cleanup_certificates("'ic_collaboration_competency'",
	"Email_Address")}}
