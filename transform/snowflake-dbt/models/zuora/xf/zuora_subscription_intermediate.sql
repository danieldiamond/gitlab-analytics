with flattening as (

    SELECT {{ dbt_utils.star(from=ref('zuora_subscription'), except=["ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY"]) }}, 
            C.value::string as ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY
    FROM {{ref('zuora_subscription')}},
    lateral flatten(input=>split(ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY, '||')) C
    WHERE lower(ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY) LIKE '%||%'

), flattened as (

    SELECT {{ dbt_utils.star(from=ref('zuora_subscription'), except=["ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY"]) }}, 
            ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY
    FROM {{ ref('zuora_subscription') }}
    WHERE lower(ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY) NOT LIKE '%||%' OR lower(ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY) IS NULL

), unioned as (

    SELECT * FROM flattening
    UNION ALL 
    SELECT * FROM flattened

), zuora_subscription as (

    SELECT *,
        row_number() OVER (
          PARTITION BY subscription_name, version
          ORDER BY updated_date DESC ) AS sub_row
    FROM unioned
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

)

SELECT
  zuora_subs_fixed.*,
  -- Dates
  date_trunc('month', zuora_subs_fixed.subscription_start_date) :: DATE AS subscription_start_month,
  date_trunc('month', dateadd('day', -1, zuora_subs_fixed.subscription_end_date))::DATE AS subscription_end_month,
  date_trunc('month', zuora_subs_fixed.contract_effective_date)::date AS subscription_month,
  date_trunc('quarter', zuora_subs_fixed.contract_effective_date)::date AS subscription_quarter,
  date_trunc('year', zuora_subs_fixed.contract_effective_date)::date AS subscription_year

FROM zuora_subs_fixed
  WHERE 
    zuora_subs_fixed.sub_row = 1 

