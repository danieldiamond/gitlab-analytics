WITH subscription_periods AS (
  
  SELECT
    *,
    subscription_version_term_start_date AS start_date,
    subscription_version_term_end_date AS end_date,
    DATEDIFF('months', subscription_version_term_start_date, subscription_version_term_end_date) AS term_length
  FROM analytics.zuora_subscription_periods
  WHERE subscription_version_term_end_date BETWEEN '2017-01-01' AND CURRENT_DATE
    AND latest_delivery = 'SaaS'
    --AND subscription_id = '2c92a0fe68a2b3ab0168bc7af49e0092'
    AND term_length = 12
),

customers AS (
  
  SELECT *
  FROM analytics_staging.customers_db_orders
  WHERE gitlab_namespace_id IS NOT NULL
    AND subscription_name_slugify IS NOT NULL

),

joined AS (

  SELECT
    subscription_periods.*,
    customers.gitlab_namespace_id
  FROM subscription_periods
    LEFT JOIN customers
      ON subscription_periods.subscription_name_slugify = customers.subscription_name_slugify
),

date_range AS (
  SELECT
    '2019-01-01'::DATE AS start_date,
    '2020-01-01'::DATE AS end_date

), 
start_accounts as (
    select
      account_id,
      sum(mrr) as total_mrr   
    from joined AS s
      inner join date_range AS d
        on s.start_date <= d.start_date     
        and (s.end_date > d.start_date or s.end_date is null)
    group by account_id
),

end_accounts as    
(
    select account_id, sum(mrr) as total_mrr    
    from joined s inner join date_range d on
        s.start_date <= d.end_date    
        and (s.end_date > d.end_date or s.end_date is null)
    group by account_id     
), 
churned_accounts as    
(
    select s.account_id, sum(s.total_mrr) as total_mrr    
    from start_accounts s 
    left outer join end_accounts e on    
        s.account_id=e.account_id
    where e.account_id is null     
    group by s.account_id        
),
downsell_accounts as     
(
    select s.account_id, s.total_mrr-e.total_mrr as downsell_amount   
    from start_accounts s 
    inner join end_accounts e on s.account_id=e.account_id     
    where e.total_mrr < s.total_mrr     
),
start_mrr as (     
    select sum (start_accounts.total_mrr) as start_mrr from start_accounts
), 
churn_mrr as (    
    select     sum(churned_accounts.total_mrr) as churn_mrr from churned_accounts
), 
downsell_mrr as (     
    select coalesce(sum(downsell_accounts.downsell_amount),0.0) as downsell_mrr    
from downsell_accounts
)
select 
    (churn_mrr+downsell_mrr) /start_mrr as mrr_churn_rate,     
    start_mrr,    
    churn_mrr, 
    downsell_mrr
from start_mrr, churn_mrr, downsell_mrr