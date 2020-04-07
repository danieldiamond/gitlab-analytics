{{ config({
    "schema": "analytics"
    })
}}

{% set partition_statement = "OVER (PARTITION BY base.diversity_field, base.aggregation_type
                              ORDER BY base.month_date DESC ROWS BETWEEN CURRENT ROW AND 11 FOLLOWING)" %}

With date_details AS (

    SELECT distinct(DATE_TRUNC('month',date_actual))    AS month_date,
            'join'                                      AS join_field
    FROM {{ref('date_details')}}
    WHERE date_actual <= {{max_date_in_bamboo_analyses()}}

), overall_headcount_pivoted AS (

    SELECT
        DATE_TRUNC('month',month_date)                  AS month_date,
        'total'                                         AS diversity_field,
        'total'                                         AS aggregation_type,
        {{ dbt_utils.pivot(
            'metric',
            dbt_utils.get_column_values(ref('bamboohr_headcount_aggregation_intermediate'), column='metric', default=[]),
            then_value ='total_count',
            quote_identifiers = False
        ) }} 
    FROM {{ ref('bamboohr_headcount_aggregation_intermediate') }}
    GROUP BY 1,2,3

), gender_headcount_pivoted AS (

    SELECT
        DATE_TRUNC('month',month_date)                  AS month_date,
        gender,
        'gender_breakdown'                              AS aggregation_type,
        {{ dbt_utils.pivot(
            'metric',
            dbt_utils.get_column_values(ref('bamboohr_headcount_aggregation_intermediate'), 'metric'),
            then_value ='total_count',
            quote_identifiers = False
        ) }}
    FROM {{ ref('bamboohr_headcount_aggregation_intermediate') }}
    GROUP BY 1,2,3

), aggregated AS (

    SELECT
        overall_headcount_pivoted.*,
        (headcount_start + headcount_end)/2             AS average_headcount
    FROM overall_headcount_pivoted

    UNION ALL

    SELECT
        gender_headcount_pivoted.*,
        (headcount_start + headcount_end)/2           AS average_headcount
    FROM gender_headcount_pivoted

), diversity_fields AS (

    SELECT
        DISTINCT diversity_field,
        aggregation_type,
        'join'                                        AS join_field
    FROM aggregated

), base AS (

    SELECT
        date_details.*,
        diversity_field,
        aggregation_type
    FROM date_details
    LEFT JOIN diversity_fields
      ON date_details.join_field = diversity_fields.join_field

), intermediate AS(

    SELECT
      base.month_date,
      base.diversity_field,
      base.aggregation_type,
      COALESCE(aggregated.headcount_start, 0)         AS headcount_start,
      COALESCE(aggregated.headcount_end, 0)           AS headcount_end,
      COALESCE(aggregated.hires,0)                    AS hires,
      AVG(COALESCE(aggregated.average_headcount, 0)) {{partition_statement}} AS rolling_12_month_headcount,
      SUM(COALESCE(total_separated,0)) {{partition_statement}}              AS rolling_12_month_separations,
      SUM(COALESCE(voluntary_separations,0)) {{partition_statement}}        AS rolling_12_month_voluntary_separations,
      SUM(COALESCE(involuntary_separations,0)) {{partition_statement}}      AS rolling_12_month_involuntary_separations
    FROM base
    LEFT JOIN aggregated
    ON base.month_date = aggregated.month_date
    AND base.diversity_field = aggregated.diversity_field
    AND base.aggregation_type = aggregated.aggregation_type

), final AS (

    SELECT
      intermediate.*,
      iff(rolling_12_month_headcount< rolling_12_month_separations, null,
        1 - (rolling_12_month_separations/NULLIF(rolling_12_month_headcount,0)))        AS retention
    FROM intermediate
    WHERE month_date > '2012-11-30'

)

SELECT *
FROM final
WHERE month_date < date_trunc('month', CURRENT_DATE)
