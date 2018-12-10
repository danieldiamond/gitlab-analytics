with base as (
		SELECT *
		FROM netsuite.accounting_periods

), renamed as (

		SELECT
            internal_id 	AS posting_period_id,
            parent_id 		AS parent_posting_period_id,

            -- all_locked
            -- allow_non_gl_changes BOOLEAN,
            -- ap_locked            BOOLEAN,
            -- ar_locked            BOOLEAN,
            -- closed               BOOLEAN,
            -- closed_on_date       TIMESTAMP,
            end_date 		AS last_day_of_month,
            -- fiscal_calendar_id
            -- fiscal_calendar_name VARCHAR,
            is_adjust,
            is_quarter,
            is_year,

            parent_name 	AS parent_period_date_name,
            -- payroll_locked
            period_name 	AS period_date_name,
            start_date 		AS first_day_of_month
            -- imported_at

		FROM base

)

SELECT *
FROM renamed