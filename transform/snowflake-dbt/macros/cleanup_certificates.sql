{%- macro cleanup_certificates(certificate_name, raw_email_column) -%}

), renamed as (

	SELECT
		"Timestamp"::timestamp::date AS date_completed,
		"Score" AS  score,
		"First_&_Last_Name" AS submitter_name,
		"{{raw_email_column}}"::STRING AS  submitter_email,
		"_UPDATED_AT" AS last_updated_at
	FROM source

), clean_score as (

	SELECT
		date_completed,
		submitter_name,
		TRIM(SPLIT_PART(score, '/', 1))::NUMBER AS correct_responses,
		TRIM(SPLIT_PART(score, '/', 2))::NUMBER AS total_responses,
    CASE WHEN LOWER(submitter_email) LIKE '%@gitlab.com%'
      THEN True
      ELSE False
      END                                   AS is_team_member,
    CASE WHEN LOWER(submitter_email) LIKE '%@gitlab.com%'
      THEN TRIM(LOWER(submitter_email))
      ELSE md5(submitter_email) END         AS submitter_email,
    {{certificate_name}}                    AS certificate_name,
		last_updated_at
	FROM renamed

)

SELECT *
FROM clean_score

{%- endmacro -%}
