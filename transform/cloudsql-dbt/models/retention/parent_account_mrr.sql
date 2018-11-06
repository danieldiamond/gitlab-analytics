with mrr_totals_levelled as ( 

	SELECT *
	FROM {{ref('mrr_totals_levelled')}}
	WHERE ultimate_parent_account_id IS NOT NULL

), grouped as (

	SELECT md5(ultimate_parent_account_id::varchar||mrr_month::varchar) as distinct_id, 
		   ultimate_parent_account_id,
	       ultimate_parent_account_name,
	       parent_account_cohort_month,
	       parent_account_cohort_quarter,
	       mrr_month,
	       months_since_parent_cohort_start,
	       quarters_since_parent_cohort_start,
	       sum(mrr) as mrr
	FROM mrr_totals_levelled
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

)

SELECT *, 
       (lag(mrr, 12) over (partition by ultimate_parent_account_id order by mrr_month))   as mrr_12mo_ago
FROM grouped

 