# Development Plan

For the MVP of BizOps, we plan to delivering the following based on [first objective](../#objectives) of the project:

* A configurable ELT engine to retrieve data out of SFDC, Zuora, and Marketo
* A BI dashboard to view the ELT'd data
* Sample dashboards to get started

This would provide a basic foundation for analyzing your CRM data that is contained within SFDC.

## Sprints

### Priority 1

For the very first MVC, we should focus on just getting an environment established which can ELT and render data:
* [Create a container with Talend and dbt, to be used as image for CI job](https://gitlab.com/gitlab-org/bizops/issues/8) (Have VM today)
  * Starts up, uses ENV vars to auth to SFDC/Zuora/Marketo, ELT's data into PG. Runs dbt to transform to data model.
* Create a container with PG and Superset (Done)
  * Is the "app" that runs as the environment
* Rely on the end user for the "extract" (App -> PG) transformation files (our version is WIP)
* Establish [standard data model](https://gitlab.com/gitlab-org/bizops/issues/9) for required fields
* Rely on the end user for "transform" (staging->data model) transformation (not yet started)
* Create the initial dashboard views based on standard model (our version not yet started)

### Priority 2

Automate & provide guide rails for ELT phase
* Create script to grab SFDC objects to create a transformation KTL file automatically (to load data into staging tables)
* Create script to check user provided mapping file for required fields (staging field -> data model field), list missing ones

### Priority 3

Make working with data easier

* Copy dashboards from the repo into Superset, to provide OOTB templates
* Identify an easy "flow" to save modified dashboard into repo. (Cut/Paste, download file, etc.)

### Backlog

* Productize this a little more, and add steps to ease the creation of the "transform" file.
* Set up backup/restore jobs for production database

### Open questions

* Should we required Marketo/Zuora data to be in SFDC, or pull from these platforms directly?
  * Pulling only from SFDC would generalize the process if customers used other tools, but then require that the integration and data is written back to SFDC
