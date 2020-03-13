WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'product_rate_plan') }}

),

renamed as (

    select

        id as product_rate_plan_id,
        productid as product_id,

        name as product_rate_plane_name,
        description as product_rate_plan_description,
        effectiveenddate::date as effective_end_date,
        effectivestartdate::date as effective_start_date,

      -- metadata
        createdbyid as created_by_id,
        createddate as created_at,
        updatedbyid as updated_by_id,
        updateddate as updated_date,
        deleted     as is_deleted

    from source

)

select * from renamed