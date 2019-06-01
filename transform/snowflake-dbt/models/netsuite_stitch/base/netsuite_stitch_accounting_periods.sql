WITH source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'accounting_periods') }}

), renamed AS (

    SELECT
        internalid                              AS accounting_period_id,
        parent['internalId']::NUMBER            AS parent_id,
        fiscalcalendar['internalId']::NUMBER    AS fiscal_calendar_id,

        periodname                              AS period_name,
        parent['name']::STRING                  AS parent_name,
        startdate                               AS start_date,
        enddate                                 AS end_date,
        fiscalcalendar['name']::STRING          AS fiscal_calendar_name,
        fiscalcalendarslist                     AS fiscal_celandars_list,
        allownonglchanges                       AS allow_non_gitlab_changes,

        isyear                                  AS is_year,
        isquarter                               AS is_quarter,
        isadjust                                AS is_adjust

    FROM source

)

SELECT *
FROM renamed