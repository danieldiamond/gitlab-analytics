/* 
    author: chase wright
    date: 6/28/2018
    summary: table creation and current manual process for uploading
    corporate dashboard metrics into gitlab dw
*/

CREATE TABLE historical.metrics(
    id int,
    date date,
    total_revenue float,
    licensed_users float,
    revenue_per_user float,
    com_paid_users float,
    core_self_host float,
    com_availability float,
    com_response_time float,
    com_active_users_thirty float,
    com_projects float,
    ending_cash float,
    ending_loc float,
    cash_change float,
    avg_monthly_burn float,
    days_outstanding float,
    cash_remaining_in_months float,
    rep_prod float,
    cac float,
    ltv float,
    ltv_to_cac_ratio float,
    cac_ratio float,
    magic_number float,
    sales_efficiency float,
    gross_burn_rate float,
    capital_consumption float,
    PRIMARY KEY(id)
);


INSERT INTO historical.metrics(id,date,total_revenue,licensed_users,revenue_per_user,	
                               com_paid_users,core_self_host,com_availability,	
                               com_response_time,com_active_users_thirty,com_projects,ending_cash,ending_loc,	
                               cash_change,avg_monthly_burn,days_outstanding,cash_remaining_in_months,	
                               rep_prod,cac,ltv,ltv_to_cac_ratio,cac_ratio,magic_number,	
                               sales_efficiency,gross_burn_rate,capital_consumption)
VALUES 
(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y)