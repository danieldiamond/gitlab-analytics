WITH zuora_account AS (
  SELECT
    *
  FROM {{ ref('zuora_account_source') }}
),
dim_dates AS (
    SELECT *
    FROM {{ ref('dim_dates') }}
),
created_date as (
  select
    cast(created_date as date) created_date,
     count(*) as num_rows
  from zuora_account
  -- Exclude this date for scale, looks like a historic data load happened then so if it's included
  -- That date is all you can really see
  -- where DATE_TRUNC("day", created_date) != '2016-07-06'
  group by cast(created_date as date)
  order by 1 ),
updated_date as
(
  select
    cast(updated_date as date) updated_date,
    count(*)as num_rows
    from zuora_account
  -- Exclude this date for scale, looks like a historic data load happened then so if it's included
  -- That date is all you can really see
  -- where DATE_TRUNC("day", updated_date) != '2018-01-19'
  group by cast(updated_date as date)
  order by 1
)

select
        distinct
          db.date_day,
          db.day_name,
          ISNULL(cd.num_rows, 0) as created_account,
          ISNULL(ud.num_rows, 0) as updated_accounts
from dim_dates db
left join created_date cd on cd.created_date = db.date_day
left join updated_date ud on ud.updated_date = db.date_day
where (cd.num_rows > 0 or ud.num_rows > 0)