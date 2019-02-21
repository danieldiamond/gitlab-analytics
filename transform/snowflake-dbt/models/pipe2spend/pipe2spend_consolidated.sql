WITH headcount AS (
    SELECT
        date_month      AS month,
        pipe            AS pipe_name,
        headcount,
        salary_per_month
    FROM {{ref('sheetload_marketing_pipe_to_spend_headcount')}}
),

pipe AS (

    SELECT
        coalesce(sales_qualified_month, sales_accepted_month, close_month)::DATE  AS month,
        pipe_name,
        sum(iacv_full_path)                                 AS pipe_iacv
    FROM {{ref('sfdc_bizible_attribution_touchpoint_xf')}}
    WHERE sales_type in ('New Business', 'Add-On Business')
      AND lead_source != 'Web Direct'
      AND month > '2018-06-01'
    GROUP BY 1, 2
),

demand_advertising_spend AS (

    SELECT
        period_date::DATE         AS month,
        CASE
          WHEN entity in ('Google','ROI DNA, Inc.','Shanghai Dragonsoft Digital Technology,Co.,LTD')
            THEN 'Google AdWords'
          WHEN entity in ('Terminus Software, Inc')
            THEN 'Terminus'
          WHEN entity in ('Facebook US','Linked In')
            THEN 'Paid Social'
          WHEN entity in ('DZone, Inc.', 'BLUESNAP INC', 'D2 Emerge LLC')
            THEN 'Paid Sponsorship'
          ELSE 'Unknown' END
                            AS pipe_name,
        sum(debit_amount)   AS spend
    FROM {{ref('netsuite_general_ledger')}}
    WHERE account_code = '6120' and debit_amount > 0
    AND period_date > '2018-03-01'
    GROUP BY 1, 2

),

field_events_spend AS (

    SELECT
        period_date         AS month,
        'Field Event'       AS pipe_name,
        sum(debit_amount)   AS spend
    FROM {{ref('netsuite_general_ledger')}}
    WHERE account_code = '6130' and debit_amount > 0
    AND period_date > '2018-03-01'
    GROUP BY 1, 2

),

joined_spend AS (

    SELECT *
    FROM demand_advertising_spend

    UNION ALL

    SELECT *
    FROM field_events_spend

)

SELECT
    h.month,
    h.pipe_name,
    CASE
      WHEN lower(h.pipe_name) IN ('google adwords','paid sponsorship','paid social','field event','terminus')
        THEN 'Paid'
      ELSE 'Organic' END    AS pipe_type,
    coalesce(p.pipe_iacv,0) AS pipe_iacv,
    h.headcount,
    h.salary_per_month,
    coalesce(j.spend, 0)    AS spend,
    coalesce(
        p.pipe_iacv/
        nullif(
            (h.salary_per_month::integer + coalesce(j.spend,0))
            ,0)
        ,0)                 AS pipe_to_spend
FROM headcount h
  LEFT JOIN pipe p ON p.month = h.month AND trim(lower(p.pipe_name)) = trim(lower(h.pipe_name))
  LEFT JOIN joined_spend j ON h.month = (j.month + INTERVAL '3 month')::date AND trim(lower(h.pipe_name)) = trim(lower(j.pipe_name))