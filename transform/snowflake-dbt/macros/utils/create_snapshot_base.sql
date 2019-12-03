{%- macro create_snapshot_base(source, primary_key, date_start, date_part, snapshot_id_name) -%}

WITH date_spine AS (

    SELECT DISTINCT
      DATE_TRUNC({{ date_part }}, date_day) AS date_actual
    FROM {{ref("date_details")}}
    WHERE date_day >= {{ date_start }}::DATE
      AND date_day <= CURRENT_DATE   

), source AS (

    SELECT *
    FROM {{ source }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ primary_key }}, DATE_TRUNC({{ date_part }}, dbt_valid_from)
                               ORDER BY dbt_valid_from DESC) = 1

), final AS (

    SELECT
      dbt_scd_id                             AS {{ snapshot_id_name }},
      date_actual,
      dbt_valid_from                         AS valid_from,
      dbt_valid_to                           AS valid_to,
      IFF(dbt_valid_to IS NULL, TRUE, FALSE) AS is_currently_valid,
      source.*
    FROM source
    INNER JOIN date_spine
      ON source.dbt_valid_from::DATE <= date_spine.date_actual
     AND (source.dbt_valid_to::DATE > date_spine.date_actual OR source.dbt_valid_to IS NULL)
    ORDER BY 2,3      
      

)

{%- endmacro -%}