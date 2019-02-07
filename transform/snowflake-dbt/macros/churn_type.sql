{%- macro churn_type() -%}

	CASE WHEN net_retention_mrr = 0 and original_mrr > 0 THEN 'Cancelled'
		 WHEN net_retention_mrr < original_mrr AND net_retention_mrr > 0 THEN 'Downgraded'
		 WHEN net_retention_mrr > original_mrr THEN 'Upgraded'
		 WHEN net_retention_mrr = original_mrr THEN 'Maintained'
		 ELSE 'Other' 
		 END AS churn_type

{%- endmacro -%}

