{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'marketing_kpi_benchmarks') }}

), renamed AS (

    SELECT
       "Goal_Date"::DATE                     AS goal_date,
       "MQL_Goal"::FLOAT                     AS mql_goal,
       "NetNewOpp_Goal"::FLOAT               AS net_new_opp_goal,
       "IACV_Large_Target"::FLOAT            AS iacv_large_target,
       "IACV_MM_Target"::FLOAT               AS iacv_mm_target,
       "IACV_SMB_Target"::FLOAT              AS iacv_smb_target,
       "SAO_Large_Target"::FLOAT             AS sao_large_target,
       "SAO_MM_Target"::FLOAT                AS sao_mm_target,
       "SAO_SMB_Target"::FLOAT               AS sao_smb_target,
       "LandedPipe_Large_Target"::FLOAT      AS landed_pipe_large_target,
       "LandedPipe_MM_Target"::FLOAT         AS landed_pipe_mm_target,
       "LandedPipe_SMB_Target"::FLOAT        AS landed_pipe_smb_target,
       "ClosedWonIACV_Large_Target"::FLOAT   AS closed_won_iacv_large_target,
       "ClosedWonIACV_MM_Target"::FLOAT      AS closed_won_iacv_mm_target,
       "ClosedWonIACV_SMB_Target"::FLOAT     AS closed_won_iacv_smb_target
    FROM source

)

SELECT *
FROM renamed
