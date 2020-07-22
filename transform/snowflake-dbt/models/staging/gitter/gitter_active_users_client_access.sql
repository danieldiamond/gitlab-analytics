-- Based off of the following references,
-- https://looker.com/platform/blocks/analytic/daily-weekly-monthly-active-users ->
-- https://discourse.looker.com/t/analytic-block-daily-weekly-monthly-active-users/1499

{{
  config(
    materialized='incremental',
    unique_key='unique_key'
  )
}}


with daily_use as (
  -- Create a table of days and activity by user_id
  select
    user_id,
    date_trunc('day', event_at) as activity_date
  from {{ ref('gitter_client_access') }}
  group by user_id, activity_date
)
-- Cross join activity and dates to build a row for each user/date combo with
-- days since last activity
select
  daily_use.user_id || '-' || wd.date_actual as unique_key,
  daily_use.user_id,
  wd.date_actual,
  min(datediff(day, daily_use.activity_date, wd.date_actual)) as days_since_last_action
from {{ ref('date_details') }} as wd
left join daily_use
  on wd.date_actual >= daily_use.activity_date
  and wd.date_actual < daily_use.activity_date + interval '30 day'
where year(wd.date_actual) >= 2013 and wd.date_actual < dateadd(year, 1, date_trunc('year', current_date()))
-- Incremental: Only transform this current months data
{% if is_incremental() %}
  and daily_use.activity_date >= DATE_TRUNC('month', CURRENT_DATE)
{% endif %}
group by 1, 2, 3
