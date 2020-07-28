<!-- Subject format should be: YYYY-MM-DD | task name | Error line from log-->
<!-- example: 2020-05-15 | dbt-non-product-models-run | Database Error in model sheetload_manual_downgrade_dotcom_tracking -->

log: <!-- link to airflow log with error -->

```
{longer error description text from log}
```

Urgency:
- [ ] T1 - Needs resolution ASAP
- [ ] T2 - Resolution Required / Keep on Milestone
- [ ] T3 - Backlog

DRIs:
- Data Analyst: 
- Data Engineer: 
 
/label ~Triage ~Infrastructure ~Break-Fix ~"Priority::1-Ops" 
