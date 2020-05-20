<!-- Subject format should be: YYYYMMDD | task name | Error line from log-->
<!-- example: 20200515 | dbt-non-product-models-run | Database Error in model sheetload_manual_downgrade_dotcom_tracking -->

log: <!-- link to airflow log with error -->

```
{longer error description text from log}
```

Urgency:
- [ ] T1 - Needs resolution ASAP
- [ ] T2 - Resolution Required / Keep on Milestone
- [ ] T3 - Backlog

/label ~Triage ~Infrastructure ~Break-Fix
