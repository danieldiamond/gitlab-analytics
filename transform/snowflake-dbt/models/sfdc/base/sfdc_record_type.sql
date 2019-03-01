{{ config(schema='analytics') }}

WITH source AS (

	SELECT *
	FROM {{ var("database") }}.salesforce_stitch.recordtype

), renamed AS(

	SELECT
		 id                as record_type_id,
		 developername     as record_type_name,
		--keys
		 businessprocessid as business_process_id,
		--info
		 name              as record_type_label,
		 description	   as record_type_description,
		 sobjecttype       as record_type_modifying_object_type

	FROM source

)

SELECT *
FROM renamed
