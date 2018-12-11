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
            from_currency	AS from_currency_id,
            from_subsidiary	AS from_subsidiary_id,
            historical_rate,
            -- is_derived
            -- is_elimination_subsidiary
            -- is_period_closed          BOOLEAN,
            posting_period 	AS posting_period_id,
            to_currency		AS to_currency_id,
            to_subsidiary	AS to_subsidiary_id
            -- imported_at

		FROM base

)

SELECT *
FROM renamed