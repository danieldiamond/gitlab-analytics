with sfdc_account as (

     SELECT * 
     FROM {{ref('sfdc_account')}}

), sfdc_users as (

     SELECT *
     FROM {{ ref('sfdc_users') }}

), sfdc_record_type as (

     SELECT *
     FROM {{ ref('sfdc_record_type') }}     

), parent_account as (

     SELECT account_name as ultimate_parent_account_name,
            account_id as ultimate_parent_account_id,
            account_segment as ultimate_parent_account_segment
     FROM {{ref('sfdc_account')}}

), joined as (

    SELECT
        sfdc_account.*,
        sfdc_users.name            as technical_account_manager,
    		parent_account.ultimate_parent_account_name, 
    		parent_account.ultimate_parent_account_segment,
        sfdc_record_type.record_type_name,
        sfdc_record_type.business_process_id,
        sfdc_record_type.record_type_label,
        sfdc_record_type.record_type_description,
        sfdc_record_type.record_type_modifying_object_type
    FROM sfdc_account
	LEFT JOIN parent_account
    ON sfdc_account.ultimate_parent_account_id = parent_account.ultimate_parent_account_id
  LEFT OUTER JOIN sfdc_users
    ON sfdc_account.technical_account_manager_id = sfdc_users.id
  LEFT JOIN sfdc_record_type
    ON sfdc_account.record_type_id = sfdc_record_type.record_type_id

)

SELECT joined.*,
      CASE
           WHEN ultimate_parent_account_segment IN('Large', 'Strategic')
                OR account_segment IN('Large', 'Strategic') THEN TRUE
           ELSE FALSE
       END AS is_large_and_up
       
FROM joined
