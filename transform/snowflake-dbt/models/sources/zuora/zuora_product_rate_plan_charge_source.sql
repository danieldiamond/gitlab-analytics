WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'product_rate_plan_charge') }}

),

renamed as (

    select

        id as product_rate_plan_charge_id,
        productid as product_id,
        ACCOUNTRECEIVABLEACCOUNTINGCODEID as account_receivable_accounting_code_id,
        ADJUSTMENTLIABILITYACCOUNTINGCODEID as adjustment_liability_accounting_code_id,
        ADJUSTMENTREVENUEACCOUNTINGCODEID as adjustment_revenue_accounting_code_id,
        CONTRACTASSETACCOUNTINGCODEID as contract_asset_accounting_code_id,
        CONTRACTLIABILITYACCOUNTINGCODEID as contract_liability_accounting_code_id,
        ADJUSTMENTREVENUEACCOUNTINGCODEID as adjustment_revenue_accounting_code_id,
        CONTRACTRECOGNIZEDREVENUEACCOUNTINGCODEID as contract_recognized_revenue_accounting_code_id,
        DEFERREDREVENUEACCOUNTINGCODEID as deffered+revenue_accounting_code_id,



        accountingcode::VARCHAR as accounting_code,
        APPLYDISCOUNTTO::VARCHAR as apply_discount_to,
        BILLCYCLETYPE::VARCHAR as bill_cycle_type,
        BILLCYCLETYPE::VARCHAR as billing_period,
        BILLINGPERIODALIGNMENT::VARCHAR as billing_period_alignment,
        BILLINGTIMING::VARCHAR as billing_timing,
        CHARGEMODEL::VARCHAR as charge_model,
        CHARGETYPE::VARCHAR as charge_type,
           DEFAULTQUANTITY::DOUBLE as default_quantity,
           DEFERREDREVENUEACCOUNT::VARCHAR as deferred_revenue_account,
           DESCRIPTION::VARCHAR as product_rate_plan_charge_description,
           DISCOUNTLEVEL::VARCHAR as discount_level,
           ENDDATECONDITION::VARCHAR as end_date_condition,
           LEGACYREVENUEREPORTING::BOOLEAN as is_legacy_revenue_reporting,
           LISTPRICEBASE::VARCHAR as list_price_base,
           OVERAGECALCULATIONOPTION::VARCHAR as overage_calculation_option
           OVERAGEUNUSEDUNITSCREDITOPTION::VARCHAR as overage_unused_units_credit_option,
           PRICECHANGEOPTION::VARCHAR as price_change_option,
           PRICEINCREASEPERCENTAGE::DOUBLE as price_increase_percentage,
           









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

             VARCHAR,
           VARCHAR,
    ADJUSTMENTREVENUEACCOUNTINGCODEID         VARCHAR,
    APPLYDISCOUNTTO                           VARCHAR,
    BILLCYCLETYPE                             VARCHAR,
    BILLINGPERIOD                             VARCHAR,
    BILLINGPERIODALIGNMENT                    VARCHAR,
    BILLINGTIMING                             VARCHAR,
    CHARGEMODEL                               VARCHAR,
    CHARGETYPE                                VARCHAR,
    CONTRACTASSETACCOUNTINGCODEID             VARCHAR,
    CONTRACTLIABILITYACCOUNTINGCODEID         VARCHAR,
    CONTRACTRECOGNIZEDREVENUEACCOUNTINGCODEID VARCHAR,
    CREATEDBYID                               VARCHAR,
    CREATEDDATE                               TIMESTAMPTZ,
    DEFAULTQUANTITY                           DOUBLE,
    DEFERREDREVENUEACCOUNT                    VARCHAR,
    DEFERREDREVENUEACCOUNTINGCODEID           VARCHAR,
    DELETED                                   BOOLEAN,
    DESCRIPTION                               VARCHAR,
    DISCOUNTLEVEL                             VARCHAR,
    ENDDATECONDITION                          VARCHAR,

    LEGACYREVENUEREPORTING                    BOOLEAN,
    LISTPRICEBASE                             VARCHAR,
    NAME                                      VARCHAR,
    OVERAGECALCULATIONOPTION                  VARCHAR,
    OVERAGEUNUSEDUNITSCREDITOPTION            VARCHAR,
    PRICECHANGEOPTION                         VARCHAR,
    PRICEINCREASEPERCENTAGE                   DOUBLE,
    PRODUCTID                                 VARCHAR,
    PRODUCTRATEPLANID                         VARCHAR,
    RATINGGROUP                               VARCHAR,
    RECOGNIZEDREVENUEACCOUNT                  VARCHAR,
    RECOGNIZEDREVENUEACCOUNTINGCODEID         VARCHAR,
    REVENUERECOGNITIONRULENAME                VARCHAR,
    REVRECCODE                                VARCHAR,
    REVRECTRIGGERCONDITION                    VARCHAR,
    SMOOTHINGMODEL                            VARCHAR,
    SPECIFICBILLINGPERIOD                     NUMBER,
    TAXABLE                                   BOOLEAN,
    TAXCODE                                   VARCHAR,
    TAXMODE                                   VARCHAR,
    TRIGGEREVENT                              VARCHAR,
    UNBILLEDRECEIVABLESACCOUNTINGCODEID       VARCHAR,
    UOM                                       VARCHAR,
    UPDATEDBYID                               VARCHAR,
    UPDATEDDATE                               TIMESTAMPTZ,
    UPTOPERIODS                               NUMBER,
    UPTOPERIODSTYPE                           VARCHAR,
    USAGERECORDRATINGOPTION                   VARCHAR,
    USEDISCOUNTSPECIFICACCOUNTINGCODE         BOOLEAN,
    USETENANTDEFAULTFORPRICECHANGE            BOOLEAN,
    WEEKLYBILLCYCLEDAY                        VARCHAR,

    from source

)

select * from renamed