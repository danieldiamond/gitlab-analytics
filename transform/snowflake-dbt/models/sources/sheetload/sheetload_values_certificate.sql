WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'values_certificate') }}

{{cleanup_certificates("'values_certificate'",
			"Email_address_(GitLab_team_members,_please_use_your_GitLab_email_address)")}}
