{{
  config( materialized='ephemeral')
}}

WITH zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_snapshots_source') }}
    WHERE {{ var('valid_at') }}>= dbt_valid_from
    AND {{ var('valid_at') }} < {{ coalesce_to_infinity('dbt_valid_to') }}

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_snapshots_source') }}
    WHERE {{ var('valid_at') }} >= dbt_valid_from
    AND {{ var('valid_at') }} < {{ coalesce_to_infinity('dbt_valid_to') }}

), base_charges AS (

    SELECT
      zuora_rate_plan_charge.rate_plan_charge_id            AS charge_id,
      zuora_rate_plan_charge.product_rate_plan_charge_id    AS product_details_id,
      zuora_rate_plan.subscription_id,
      zuora_rate_plan_charge.account_id,
      zuora_rate_plan_charge.rate_plan_charge_number,
      zuora_rate_plan_charge.rate_plan_charge_name,
      zuora_rate_plan_charge.effective_start_month,
      zuora_rate_plan_charge.effective_end_month,
      TO_NUMBER(TO_CHAR(zuora_rate_plan_charge.effective_start_date, 'YYYYMMDD'),'99999999')
                                                            AS effective_start_date_id,
      TO_NUMBER(TO_CHAR(zuora_rate_plan_charge.effective_end_date, 'YYYYMMDD'), '99999999')
                                                            AS effective_end_date_id,
      TO_NUMBER(TO_CHAR(zuora_rate_plan_charge.effective_start_month, 'YYYYMMDD'),'99999999')
                                                            AS effective_start_month_id,
      TO_NUMBER(TO_CHAR(zuora_rate_plan_charge.effective_end_month, 'YYYYMMDD'), '99999999')
                                                            AS effective_end_month_id,
      zuora_rate_plan_charge.unit_of_measure,
      zuora_rate_plan_charge.quantity,
      zuora_rate_plan_charge.mrr,
      zuora_rate_plan_charge.delta_tcv,
      zuora_rate_plan.rate_plan_name                        AS rate_plan_name,
      {{ product_category('zuora_rate_plan.rate_plan_name') }},
      {{ delivery('product_category')}},
      CASE
        WHEN lower(rate_plan_name) like '%support%'
          THEN 'Support Only'
        ELSE 'Full Service'
      END                                                   AS service_type,
      zuora_rate_plan_charge.discount_level,
      zuora_rate_plan_charge.segment                        AS rate_plan_charge_segment,
      zuora_rate_plan_charge.version                        AS rate_plan_charge_version,
      zuora_rate_plan_charge.charge_type
    FROM zuora_rate_plan
    INNER JOIN zuora_rate_plan_charge
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id

)

SELECT *
FROM base_charges
