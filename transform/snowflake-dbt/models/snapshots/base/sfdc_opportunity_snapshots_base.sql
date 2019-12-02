{{ config({
    "alias": "sfdc_opportunity_snapshots",
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'sfdc_opportunity_snapshots') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id, DATE_TRUNC('day', dbt_valid_from) ORDER BY dbt_valid_from DESC) = 1   

), date_spine AS (

    SELECT
      date_actual
    FROM {{ref("date_details")}}
    WHERE date_actual >= '2019-10-01'::DATE
      AND date_actual <= CURRENT_DATE
    ORDER BY 1

), final AS (

    SELECT
      dbt_scd_id::VARCHAR                    AS opportunity_snapshot_id, 
      date_actual,
      source.*,
      IFF(dbt_valid_to IS NULL, TRUE, FALSE) AS is_current_snapshot,
      dbt_valid_from                         AS valid_from,
      dbt_valid_to                           AS valid_to
    FROM source
    INNER JOIN date_spine
      ON source.dbt_valid_from::DATE <= date_spine.date_actual
     AND (source.dbt_valid_to > date_spine.date_actual OR source.dbt_valid_to IS NULL)  

)

SELECT *
FROM final