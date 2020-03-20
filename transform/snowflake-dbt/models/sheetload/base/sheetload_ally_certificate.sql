WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'ally_certificate') }}

{{cleanup_certificates("'ally_certificate'",
	"Email_Address_(GitLab_team_members,_please_use_your_GitLab_email_address)")}}
