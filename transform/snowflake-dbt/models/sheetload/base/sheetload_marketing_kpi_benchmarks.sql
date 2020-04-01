{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'marketing_kpi_benchmarks') }}

), renamed AS (

    SELECT
      "Goal_Date"::DATE                               AS goal_date,
      "MQL_Goal"::FLOAT                               AS mql_goal,
      NULLIF("NetNewOpp_Goal", '')::FLOAT             AS net_new_opp_goal,
      NULLIF("IACV_Large_Target", '')::FLOAT          AS iacv_large_target,
      NULLIF("IACV_MM_Target", '')::FLOAT             AS iacv_mm_target,
      NULLIF("IACV_SMB_Target", '')::FLOAT            AS iacv_smb_target,
      NULLIF("SAO_Large_Target", '')::FLOAT	          AS sao_large_target,
      NULLIF("SAO_MM_Target", '')::FLOAT              AS sao_mm_target,
      NULLIF("SAO_SMB_Target", '')::FLOAT	          AS sao_smb_target,
      NULLIF("LandedPipe_Large_Target", '')::FLOAT    AS landed_pipe_large_target,
      NULLIF("LandedPipe_MM_Target", '')::FLOAT       AS landed_pipe_mm_target,
      NULLIF("LandedPipe_SMB_Target", '')::FLOAT	  AS landed_pipe_smb_target,
      NULLIF("ClosedWonIACV_Large_Target", '')::FLOAT AS closed_won_iacv_large_target,
      NULLIF("ClosedWonIACV_MM_Target", '')::FLOAT    AS closed_won_iacv_mm_target,
      NULLIF("ClosedWonIACV_SMB_Target", '')::FLOAT   AS closed_won_iacv_smb_target,
      NULLIF("MQL_Large_Goal", '')::FLOAT             AS 
      NULLIF("MQL_MM_Goal", '')::FLOAT                AS 
      NULLIF("MQL_SMB_Goal", '')::FLOAT               AS 
      NULLIF("MQL_To_SAO_Conversion_Large", '')::FLOAT  
                                                      AS 
      NULLIF("MQL_To_SAO_Conversion_MM", '')::FLOAT   AS 
      NULLIF("MQL_To_SAO_Conversion_SMB", '')::FLOAT  AS 
      NULLIF("MQL_To_SAO_Conversion_SMB", '')::FLOAT  AS
      NULLIF("MQL_To_SAO_Conversion_SMB", '')::FLOAT  AS
      NULLIF("MQL_To_SAO_Conversion_SMB", '')::FLOAT  AS
      NULLIF("MQL_To_SAO_Conversion_SMB", '')::FLOAT  AS
      NULLIF("MQL_To_SAO_Conversion_SMB", '')::FLOAT  AS
      NULLIF("MQL_To_SAO_Conversion_SMB", '')::FLOAT  AS
      NULLIF("MQL_To_SAO_Conversion_SMB", '')::FLOAT  AS
      
      
    FROM source

)

SELECT *
FROM renamed
