-- fail if the row_count between today's snapshot and yesterday's snapshot
-- fluctuates by more than 10%

with latest_snapshot as (
  
  select max(snapshot_date) as max_snapshot_date, count(*) as current_row_count 
  from analytics.f_snapshot_opportunity
  where snapshot_date = (select max(snapshot_date) from analytics.snapshot_opportunity)
  
), second_latest_snapshot as (

  select snapshot_date as second_max_snapshot_date, count(*) as previous_row_count
  from analytics.f_snapshot_opportunity
  where snapshot_date = (select dateadd('day', -1, max(snapshot_date)) from analytics.snapshot_opportunity)
  group by 1

)

select *
from latest_snapshot ls
join second_latest_snapshot sls
where (1.1 * previous_row_count) < current_row_count
or (0.9 * previous_row_count) > current_row_count
