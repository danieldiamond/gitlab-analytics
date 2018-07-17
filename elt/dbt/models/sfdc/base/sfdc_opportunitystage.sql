WITH source AS ( 

	SELECT *
	FROM sfdc.opportunitystage

), mapped_stages AS (

    SELECT
        id,
        CASE
          WHEN id = '01J6100000Jf6oDEAR' -- 0-Pending Acceptance
            THEN '0-Pending Acceptance'
          WHEN id = '01J6100000G5Xj4EAF' -- BDR Qualified
            THEN '0-Pending Acceptance'
          WHEN id = '01J6100000Ip3TXEAZ' -- 0-Pre AE Qualified
            THEN '0-Pending Acceptance'
          WHEN id = '01J6100000B8YLFEA3' -- Discovery
            THEN '1-Discovery'
          WHEN id = '01J6100000Ip3ThEAJ' -- 1-Discovery
            THEN '1-Discovery'
          WHEN id = '01J6100000B8YLPEA3' -- Developing
            THEN '2-Scoping'
          WHEN id = '01J6100000Ip3TmEAJ' -- 2-Scoping
            THEN '2-Scoping'
          WHEN id = '01J6100000B8YLUEA3' -- Present Solution
            THEN '3-Technical Evaluation'
          WHEN id = '01J6100000Ip3TrEAJ' -- 3-Tehcnical Evaluation
            THEN '3-Technical Evaluation'
          WHEN id = '01J6100000Ip3UBEAZ' -- 4-Proposal
            THEN '4-Proposal'
          WHEN id = '01J6100000B8YLZEA3' -- Negotiating
            THEN '5-Negotiating'
          WHEN id = '01J6100000IoytuEAB' -- 5-Negotiating
            THEN '5-Negotiating'
          WHEN id = '01J6100000B8YLjEAN' -- Verbal Commitment
            THEN '6-Awaiting Signature'
          WHEN id = '01J6100000IWdyOEAT' -- Awaiting Approval
            THEN '6-Awaiting Signature'
          WHEN id = '01J6100000JfUIpEAN' -- 6-Awaiting Signature
            THEN '6-Awaiting Signature'
          WHEN isclosed IS TRUE
            THEN '7-Closed'
          ELSE
            'Unmapped'
        END                     AS mapped_stage
        FROM source

)

, renamed AS(

	SELECT
        row_number()
          OVER (
            ORDER BY source.id )       AS stage_id,

        -- keys
        source.id                      AS sfdc_id,

        -- logistical info
        -- apiname equals masterlabel as of 2018-05-24
        masterlabel             AS primary_label,
        m.mapped_stage          AS mapped_stage,
        CASE
          WHEN m.mapped_stage IN ('1-Discovery','2-Scoping','3-Technical Evaluation')
            THEN 'Pipeline'
          WHEN m.mapped_stage IN ('4-Proposal')
            THEN 'Best Case'
          WHEN m.mapped_stage IN ('5-Negotiating','6-Awaiting Signature')
            THEN 'Commit'
          WHEN m.mapped_stage IN ('7-Closed')
            THEN 'Closed'
          ELSE
            'Unmapped'
        END                     AS pipeline_state,
        defaultprobability      AS default_probability,
        isactive                AS is_active,
        isclosed                AS is_closed,
        iswon                   AS is_won,
        CASE
          WHEN isclosed = TRUE AND iswon = TRUE
            THEN 'Won'
          WHEN isclosed = TRUE AND iswon = FALSE
            THEN 'Lost'
          WHEN isclosed = FALSE AND iswon = FALSE
            THEN 'Open'
          ELSE
            'Unknown'
        END                     AS stage_state

	FROM source
	JOIN mapped_stages m ON m.id = source.id

)

SELECT *
FROM renamed