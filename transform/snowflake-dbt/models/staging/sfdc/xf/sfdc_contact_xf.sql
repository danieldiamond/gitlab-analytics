with sfdc_contact as (

     SELECT
	{{ dbt_utils.star(from=ref('sfdc_contact'), except=["CONTACT_EMAIL", "CONTACT_NAME"]) }}
     FROM {{ref('sfdc_contact')}}

), sfdc_record_type as (

     SELECT *
     FROM {{ ref('sfdc_record_type') }}     

), joined as (

    SELECT
        sfdc_contact.*,
        sfdc_record_type.business_process_id,
        sfdc_record_type.record_type_label,
        sfdc_record_type.record_type_description,
        sfdc_record_type.record_type_modifying_object_type
    FROM sfdc_contact
    LEFT JOIN sfdc_record_type
    ON sfdc_contact.record_type_id = sfdc_record_type.record_type_id

)

SELECT *       
FROM joined
