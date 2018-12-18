WITH zuora_subscription AS (

    SELECT
        *,
        row_number()
        OVER (
          PARTITION BY
            subscription_name,
            version
          ORDER BY updated_date DESC ) AS sub_row
    FROM {{ ref('zuora_subscription') }}
     /*
      The partition deduplicates the subscriptions when there are
      more than one version at the same time.
      See account_id = '2c92a0fc55a0dc530155c01a026806bd' for
      an example.
     */

), zuora_subs_fixed AS (

  SELECT * 
  FROM zuora_subscription
  WHERE subscription_status IN ('Active', 'Cancelled')

), renewal_subs AS (

    SELECT
      zuora_renewal_subscription_name_slugify               AS renewal_subscription_slug,
      subscription_name_slugify                             AS parent_subscription_slug,
      contract_effective_date                               AS renewal_contract_start_date,
      row_number() OVER (
                    PARTITION BY
                      zuora_renewal_subscription_name_slugify
                    ORDER BY contract_effective_date ASC )  AS renewal_row
    FROM zuora_subs_fixed
    WHERE zuora_renewal_subscription_name_slugify IS NOT NULL
      AND sub_row = 1

)

SELECT
  zuora_subs_fixed.*,

  coalesce(parent_subscription_slug, zuora_subs_fixed.subscription_name_slugify) AS subscription_slug_for_counting,
  -- Dates
  date_trunc('month', zuora_subs_fixed.subscription_start_date) :: DATE                         AS subscription_start_month,
  dateadd('month', -1, zuora_subs_fixed.subscription_end_date)::DATE AS subscription_end_month,

  -- This properly links a subscription to its renewal so that future subscriptions inherit the proper cohorts.
  CASE
    WHEN zuora_subs_fixed.contract_effective_date < renewal_subs.renewal_contract_start_date
      THEN date_trunc('month', zuora_subs_fixed.contract_effective_date) :: DATE
    ELSE date_trunc('month', coalesce(renewal_subs.renewal_contract_start_date,zuora_subs_fixed.contract_effective_date)) :: DATE END        AS cohort_month,

  CASE
    WHEN zuora_subs_fixed.contract_effective_date < renewal_subs.renewal_contract_start_date
      THEN date_trunc('quarter', zuora_subs_fixed.contract_effective_date) :: DATE
    ELSE date_trunc('quarter', coalesce(renewal_subs.renewal_contract_start_date,zuora_subs_fixed.contract_effective_date)) :: DATE END      AS cohort_quarter,

  CASE
    WHEN zuora_subs_fixed.contract_effective_date < renewal_subs.renewal_contract_start_date
      THEN date_trunc('year', zuora_subs_fixed.contract_effective_date) :: DATE
    ELSE date_trunc('year', coalesce(renewal_subs.renewal_contract_start_date,zuora_subs_fixed.contract_effective_date)) :: DATE END         AS cohort_year

FROM zuora_subs_fixed
  LEFT JOIN renewal_subs ON renewal_subs.renewal_subscription_slug = zuora_subs_fixed.subscription_name_slugify
  WHERE 
    zuora_subs_fixed.sub_row = 1 
    AND (renewal_subs.renewal_row = 1 OR renewal_subs.renewal_row is NULL)
