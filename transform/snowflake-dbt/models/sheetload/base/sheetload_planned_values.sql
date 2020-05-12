WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'planned_values') }}

), renamed AS (

    SELECT
      unique_key::INT                       AS primary_key,
      plan_month::DATE                      AS plan_month,
      planned_new_pipe::INT                 AS planned_new_pipe,
      planned_total_iacv::INT               AS planned_total_iacv,
      planned_tcv_minus_gross_opex::INT     AS planned_tcv_minus_gross_opex,
      planned_total_arr::INT                AS planned_total_arr,
      sales_efficiency_plan::FLOAT          AS sales_efficiency_plan,
      magic_number_plan::FLOAT              AS magic_number_plan,
      tcv_plan::INT                         AS tcv_plan,
      acv_plan::INT                         AS acv_plan,
      planned_new_iacv::INT                 AS planned_new_iacv,
      planned_growth_iacv::INT              AS planned_growth_iacv,
      iacv_divided_by_capcon_plan::FLOAT    AS iacv_divided_by_capcon_plan,
      planned_iacv_ent_apac::INT            AS planned_iacv_ent_apac,
      planned_iacv_ent_emea::INT            AS planned_iacv_ent_emea,
      planned_iacv_ent_pubsec::INT          AS planned_iacv_ent_pubsec,
      planned_iacv_ent_us_west::INT         AS planned_iacv_ent_us_west,
      planned_iacv_ent_us_east::INT         AS planned_iacv_ent_us_east,
      planned_iacv_mm::INT                  AS planned_iacv_mm,
      planned_iacv_smb::INT                 AS planned_iacv_sbm
    FROM source

)

SELECT *
FROM renamed
