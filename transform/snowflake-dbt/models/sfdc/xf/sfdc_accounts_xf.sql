WITH sfdc_account AS (

    SELECT * 
    FROM {{ ref('sfdc_account') }}

), sfdc_users AS (

    SELECT *
    FROM {{ ref('sfdc_users') }}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}     

), sfdc_account_deal_size_segmentation AS (

    SELECT *
    FROM {{ ref('sfdc_account_deal_size_segmentation') }}

), parent_account AS (

    SELECT 
      account_id      AS ultimate_parent_account_id,
      account_name    AS ultimate_parent_account_name,
      account_segment AS ultimate_parent_account_segment
    FROM {{ ref('sfdc_account') }}

), joined AS (

    SELECT
      sfdc_account.*,
      sfdc_users.name AS technical_account_manager,
      parent_account.ultimate_parent_account_name, 
      parent_account.ultimate_parent_account_segment,
      sfdc_record_type.record_type_name,
      sfdc_record_type.business_process_id,
      sfdc_record_type.record_type_label,
      sfdc_record_type.record_type_description,
      sfdc_record_type.record_type_modifying_object_type,
      sfdc_account_deal_size_segmentation.deal_size,
      CASE 
        WHEN ultimate_parent_account_segment IN ('Large', 'Strategic')
          OR account_segment IN ('Large', 'Strategic') 
          THEN TRUE
        ELSE FALSE 
      END             AS is_large_and_up
    FROM sfdc_account
    LEFT JOIN parent_account
      ON sfdc_account.ultimate_parent_account_id = parent_account.ultimate_parent_account_id
    LEFT OUTER JOIN sfdc_users
      ON sfdc_account.technical_account_manager_id = sfdc_users.id
    LEFT JOIN sfdc_record_type
      ON sfdc_account.record_type_id = sfdc_record_type.record_type_id
    LEFT JOIN sfdc_account_deal_size_segmentation
      ON sfdc_account.account_id = sfdc_account_deal_size_segmentation.account_id

)

SELECT *
FROM joined
