{% set levels = ['zuora_subscription_id',
                  'sfdc_account_id',
                  'parent_account_id'] %}

with base as (
    SELECT oldest_subscription_in_cohort as zuora_subscription_id,
            ultimate_parent_account_id as parent_account_id,
          {{ dbt_utils.star(from=ref('mrr_totals_levelled'), 
            except=["oldest_subscription_in_cohort", "ultimate_parent_account_id"]) }}
    FROM {{ref('mrr_totals_levelled')}}

{% for level in levels -%} 
), {{level}}_max_month as (

    SELECT max(mrr_month) as most_recent_mrr_month,
              {{ level }}  AS id
    FROM base
    WHERE mrr_month < dateadd(month, -1, CURRENT_DATE)
    GROUP BY 2

), {{level}}_get_mrr as(

    SELECT {{level}}_max_month.*, sum(base.mrr) as mrr
    FROM {{level}}_max_month
    LEFT JOIN base
        ON {{level}}_max_month.id =
              base.{{level}}
        AND {{level}}_max_month.most_recent_mrr_month = base.mrr_month
    GROUP BY 1, 2

), {{level}}_get_segmentation as (

SELECT id,
      '{{level}}'::varchar as level_,
      CASE WHEN (mrr*12) < 5000 THEN 'Under 5K'
          WHEN (mrr*12) < 50000 THEN '5K to 50K'
          WHEN (mrr*12) < 100000 THEN '50K to 100K'
          WHEN (mrr*12) < 500000 THEN '100K to 500K'
          WHEN (mrr*12) < 1000000 THEN '500K to 1M'
          ELSE '1M and above'
      END AS arr_segmentation
FROM {{level}}_get_mrr
GROUP BY 1, 2, 3

{% endfor -%}
), unioned as (

{% for level in levels -%} 

SELECT * FROM {{level}}_get_segmentation
{%- if not loop.last %} UNION ALL {%- endif %}

{% endfor -%}
)
SELECT * FROM unioned
GROUP BY 1, 2, 3
