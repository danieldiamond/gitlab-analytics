WITH zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}

), base_charges AS (

    SELECT
      zuora_rate_plan_charge.rate_plan_charge_id,
      zuora_rate_plan.product_id,
      zuora_rate_plan_charge.rate_plan_charge_number,
      zuora_rate_plan_charge.rate_plan_charge_name,
      zuora_rate_plan.rate_plan_name                      AS rate_plan_name,
      zuora_rate_plan_charge.segment                      AS rate_plan_charge_segment,
      zuora_rate_plan_charge.version                      AS rate_plan_charge_version,
      zuora_rate_plan_charge.effective_start_date::DATE   AS effective_start_date,
      zuora_rate_plan_charge.effective_end_date::DATE     AS effective_end_date,
      zuora_rate_plan_charge.unit_of_measure,
      zuora_rate_plan_charge.quantity,
      zuora_rate_plan_charge.mrr,
      {{ product_category('zuora_rate_plan.rate_plan_name') }}
    FROM zuora_rate_plan
    INNER JOIN zuora_rate_plan_charge
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
   WHERE zuora_rate_plan_charge.account_id NOT IN ({{ zuora_excluded_accounts() }})

), final AS (

    SELECT
      base_charges.*,
      ROW_NUMBER() OVER (
          PARTITION BY rate_plan_charge_number
          ORDER BY rate_plan_charge_segment, rate_plan_charge_version
      ) AS segment_version_order,
      IFF(ROW_NUMBER() OVER (
          PARTITION BY rate_plan_charge_number, rate_plan_charge_segment
          ORDER BY rate_plan_charge_version DESC) = 1,
          TRUE, FALSE
      ) AS is_last_segment_version
    FROM base_charges
)

SELECT *
FROM final