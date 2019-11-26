{{ config({
    "alias": "sfdc_opportunity_snapshots",
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id, DATE_TRUNC('day', dbt_valid_from) ORDER BY dbt_valid_from DESC) AS rank_in_day
    FROM {{ source('snapshots', 'sfdc_opportunity_snapshots') }}   

), date_spine AS (

    SELECT
      date_actual
    FROM {{ref("date_details")}}
    WHERE date_actual >= '2019-10-01'::DATE
      AND date_actual <= CURRENT_DATE
    ORDER BY 1

), final AS (

    SELECT DISTINCT
      date_actual,
      source.*,
      dbt_valid_from                         AS valid_from,
      dbt_valid_to                           AS vaid_to,
      IFF(dbt_valid_to IS NULL, TRUE, FALSE) AS is_current_snapshot
    FROM snapshots
    INNER JOIN date_spine
      ON snapshots.valid_from <= date_spine.date_actual
     AND (snapshots.valid_to > date_spine.date_actual OR snapshots.is_current_snapshot = TRUE)
     AND snapshots.rank_in_day = 1   

)

SELECT *
FROM final