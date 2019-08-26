This extract pulls data for commit stats and red master stats. Original issue was https://gitlab.com/gitlab-data/analytics/issues/1815

Tables were created using the following command:

```sql
create or replace table raw.engineering_extracts.red_master_stats (
    jsontext variant,
    uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
  );
  ```