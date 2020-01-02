WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'account') }}

), materialized AS (

    SELECT *
    FROM {{ ref('sfdc_account') }}

), compared AS (

    SELECT DISTINCT 
        source.id, 
        source.createddate, 
        source.ultimate_parent_account_id__c, 
        substring(
            regexp_replace(
                source.ultimate_parent_account_id__c,'_HL_ENCODED_/|<a\\s+href="/', ''
            )
            , 0, 15)                                     AS regex_version,
        materialized.account_id, 
        materialized.utimate_parent_id, 
        materialized.ultimate_parent_account_id, 
        materialized.created_date, 
        CASE 
        WHEN source.id IS NOT NULL THEN 0
            ELSE 1 END                                   AS is_missing_from_raw, 
        CASE 
        WHEN materialized.account_id IS NOT NULL THEN 0
            ELSE 1 END                                   AS is_missing_from_data_model,
        CASE 
        WHEN source.ultimate_parent_account_id__c = materialized.utimate_parent_id THEN 0 
            ELSE 1 END                                   AS is_ultimate_parent_account_id_mismatch,
        CASE 
        WHEN trim(substring(regexp_replace(source.ultimate_parent_account_id__c,'_HL_ENCODED_/|<a\\s+href="/', ''), 0, 15)) = trim(materialized.ultimate_parent_account_id) THEN 0
            ELSE 1 END                                   AS is_ultimate_parent_account_id_mismatch_regex, 
        CASE 
        WHEN source.createddate = materialized.created_date THEN 0 
            ELSE 1 END                                   AS is_created_date_mismatch
    FROM source
    LEFT JOIN materialized
    ON source.id = materialized.account_id
        AND source.isdeleted = FALSE 
        AND source.id IS NOT NULL

)

SELECT *
FROM compared
