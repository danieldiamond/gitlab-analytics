WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'product_rate_plan') }}

),

renamed as (

    select

        id as product_rate_plan_id,
        productid as product_id,
        rateplansku__c as rate_plan_sku,

        rows_allowed__c as rows_allowed,
        integrations_enabled__c as integrations_enabled,
        log_days__c as log_days,

        name,
        description,
        effectiveenddate::date as effective_end_date,
        effectivestartdate::date as effective_start_date,

        deleted,

        createdbyid as created_by_id,
        createddate as created_at,
        updatedbyid as updated_by_id,
        updateddate as updated_date

    from source

)

select * from renamed