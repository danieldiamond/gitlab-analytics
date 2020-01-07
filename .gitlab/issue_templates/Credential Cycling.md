<!---
This issue is for cycling credentials within our data systems.
---->

[Google Sheet for Further Documentation](https://docs.google.com/spreadsheets/d/17T89cBIDLkMUa3rIw1GxS-QWFL7kjeLj2rCQGZLEpyA/edit?usp=sharing)

#### Places that credentials need to be cycled:

* [ ] 1Password
* [ ] Analytics project CI Variables
* [ ] Data Infrastructure project CI Variables
* [ ] Data Utils project CI Variables
* [ ] Chatops project CI Variables
* [ ] Kube secrets (default namespace)
* [ ] Kube secrets (testing namespace)
* [ ] Stitch
* [ ] Fivetran

#### Data Refresh

* [ ] Fivetran
  * [ ] Drop tables in schema
  * [ ] Trigger full-refresh in UI
* [ ] Stitch
  * [ ] Drop tables in schema
  * [ ] Trigger full-refresh in UI
