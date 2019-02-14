with sfdc_opportunity  as (

		SELECT *
		FROM {{ref('sfdc_opportunity_xf')}}

), base as (

		SELECT account_id,
				max(CLOSE_DATE) as close_date
		FROM sfdc_opportunity
		WHERE close_date < CURRENT_DATE
		AND is_won = True
		AND stage_is_closed = True
    	GROUP BY 1

), xf as (

		SELECT base.*, 
				max(sfdc_opportunity.incremental_acv) as incremental_acv
		FROM base
		LEFT JOIN sfdc_opportunity
		ON sfdc_opportunity.account_id = base.account_id
		AND sfdc_opportunity.close_date::date = base.close_date::date
		GROUP BY 1, 2

), final as (

		SELECT *, 
		        {{sfdc_deal_size('incremental_acv', 'deal_size')}}
		FROM xf

)

SELECT *
FROM final