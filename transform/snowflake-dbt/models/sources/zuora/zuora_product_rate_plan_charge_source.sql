WITH source AS (
    SELECT *
    FROM {{ source('zuora', 'product_rate_plan_charge') }}

),

renamed as (

select
    id as product_rate_plan_charge_id,
    productid as product_id,
    accountreceivableaccountingcodeid as account_receivable_accounting_code_id,
    adjustmentliabilityaccountingcodeid as adjustment_liability_accounting_code_id,
    adjustmentrevenueaccountingcodeid as adjustment_revenue_accounting_code_id,
    contractassetaccountingcodeid as contract_asset_accounting_code_id,
    contractliabilityaccountingcodeid as contract_liability_accounting_code_id,
    contractrecognizedrevenueaccountingcodeid as contract_recognized_revenue_accounting_code_id,
    deferredrevenueaccountingcodeid as deffered_revenue_accounting_code_id,
    PRODUCTRATEPLANID::VARCHAR AS product_rate_plan_id,
    RECOGNIZEDREVENUEACCOUNTINGCODEID::VARCHAR as recognized_revenue_account_code_id,
    accountingcode::VARCHAR as accounting_code,
    applydiscountto::VARCHAR as apply_discount_to,
    billcycletype::VARCHAR as bill_cycle_type,
    billcycletype::VARCHAR as billing_period,
    billingperiodalignment::VARCHAR as billing_period_alignment,
    billingtiming::VARCHAR as billing_timing,
    chargemodel::VARCHAR as charge_model,
    CHARGETYPE::VARCHAR as charge_type,
    DEFAULTQUANTITY::DOUBLE as default_quantity,
    DEFERREDREVENUEACCOUNT::VARCHAR as deferred_revenue_account,
    DESCRIPTION::VARCHAR as product_rate_plan_charge_description,
    DISCOUNTLEVEL::VARCHAR as discount_level,
    ENDDATECONDITION::VARCHAR as end_date_condition,
    LEGACYREVENUEREPORTING::BOOLEAN as is_legacy_revenue_reporting,
    LISTPRICEBASE::VARCHAR as list_price_base,
    name::VARCHAR as product_rate_plan_charge_name,
    OVERAGECALCULATIONOPTION::VARCHAR as overage_calculation_option,
    OVERAGEUNUSEDUNITSCREDITOPTION::VARCHAR as overage_unused_units_credit_option,
    PRICECHANGEOPTION::VARCHAR as price_change_option,
    PRICEINCREASEPERCENTAGE::DOUBLE as price_increase_percentage,
    RATINGGROUP::VARCHAR as rating_group,
    RECOGNIZEDREVENUEACCOUNT::VARCHAR as recognized_revenue_account,
    REVENUERECOGNITIONRULENAME::VARCHAR as revenue_recognition_rule_name,
    REVRECCODE::VARCHAR as revrec_code,
    REVRECTRIGGERCONDITION::VARCHAR as revrec_trigger_condition,
    SMOOTHINGMODEL::VARCHAR as smoothing_model,
    SPECIFICBILLINGPERIOD::VARCHAR as specific_billing_period,
    TAXABLE::BOOLEAN as is_taxable,
    TAXCODE::VARCHAR as tax_code,
    taxmode::VARCHAR as tax_mode,
    TRIGGEREVENT::VARCHAR as trigger_event,
    WEEKLYBILLCYCLEDAY::VARCHAR as weekly_bill_cycle_day,
    description::VARCHAR as product_rate_plan_description,
    effectiveenddate::date as effective_end_date,
    effectivestartdate::date as effective_start_date,

    -- metadata
    createdbyid as created_by_id,
    createddate as created_at,
    updatedbyid as updated_by_id,
    updateddate as updated_date,
    deleted as is_deleted


from source

    )

select *
from renamed