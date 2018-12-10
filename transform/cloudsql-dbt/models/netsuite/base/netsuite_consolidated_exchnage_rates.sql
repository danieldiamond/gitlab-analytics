with base as (
		SELECT *
		FROM netsuite.consolidated_exchange_rates

), renamed as (

		SELECT
            internal_id 	AS internal_id,
            -- external_id
            -- accounting_book
            average_rate,
            current_rate,
            from_currency,
            from_subsidiary,
            historical_rate,
            -- is_derived
            -- is_elimination_subsidiary
            -- is_period_closed          BOOLEAN,
            posting_period,
            to_currency,
            to_subsidiary
            -- imported_at

		FROM base

)

SELECT *
FROM renamed