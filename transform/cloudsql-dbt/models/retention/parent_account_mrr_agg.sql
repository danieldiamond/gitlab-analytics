with parent_account_mrr as (
	SELECT *
	FROM {{ref('parent_account_mrr')}}
	WHERE mrr_12mo_ago is not null
)

SELECT mrr_month,
       sum(mrr)/nullif(sum(mrr_12mo_ago),0)                                                as net_retention,
       sum((CASE WHEN mrr > mrr_12mo_ago THEN mrr_12mo_ago ELSE mrr END))/nullif(sum(mrr_12mo_ago),0) as gross_retention
FROM parent_account_mrr
GROUP BY 1
