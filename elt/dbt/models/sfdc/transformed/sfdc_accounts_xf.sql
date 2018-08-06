with sfdc_account as (

     SELECT * 
     FROM {{ref('sfdc_account')}}

), parent_account as (

     SELECT account_name as ultimate_parent_account_name,
            account_id as ultimate_parent_account_id,
            account_segment as ultimate_parent_account_segment     
     FROM {{ref('sfdc_account')}}

), joined as (

    SELECT sfdc_account.*, 
    		parent_account.ultimate_parent_account_name, 
    		parent_account.ultimate_parent_account_segment
    FROM sfdc_account
	LEFT JOIN parent_account
    ON sfdc_account.ultimate_parent_account_id = parent_account.ultimate_parent_account_id

)

SELECT joined.*,
      CASE
           WHEN ultimate_parent_account_segment IN('Large', 'Strategic')
                OR account_segment IN('Large', 'Strategic') THEN TRUE
           ELSE FALSE
       END AS is_large_and_up
FROM joined
