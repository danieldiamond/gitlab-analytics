with mrr_totals as (

	SELECT *
	FROM {{ref('zuora_mrr_totals_levelled')}}
)

SELECT mrr_month,
       sum(mrr)/nullif(sum(mrr_12mo_ago),0)                                                as net_retention,
       sum((CASE WHEN mrr > mrr_12mo_ago THEN mrr_12mo_ago ELSE mrr END))/nullif(sum(mrr_12mo_ago),0) as gross_retention
FROM mrr_totals
WHERE mrr_12mo_ago is not null
GROUP BY 1
