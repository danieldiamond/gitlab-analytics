WITH source AS (

	SELECT *
	FROM sfdc.recordtype

), renamed AS(

	SELECT
				 id                as record_type_id,
				 developername     as record_type_name,
			--keys
				 businessprocessid as business_process_id,
			--info
				 name              as record_type_label,
				 description,
				 sobjecttype       as modifying_object_type,
				 namespaceprefix   as prefix,
			--metadata
				 lastmodifiedbyid  as last_modified_by_id,
				 createdbyid       as created_by_id,
				 createddate       as created_date,
				 isactive          as is_active

	FROM source

)

SELECT *
FROM renamed
