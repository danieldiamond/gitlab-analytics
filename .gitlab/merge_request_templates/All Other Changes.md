## Issue
<!---
Link the Issue this MR closes
--->
Closes #

## Solution

Describe the solution.

### Related Links

Please include links to any related MRs and/or issues.

## All MRs Checklist
* [ ] [Label hygiene](https://about.gitlab.com/handbook/business-ops/data-team/#issue-labeling) on issue
* [ ] Pipelines pass
* [ ] Branch set to delete
* [ ] Commits NOT set to squash
* [ ] This MR is ready for final review and merge.
* [ ] Resolve all threads
* [ ] Remove the `WIP:` prefix in the MR title before assigning to reviewer
* [ ] Assigned to reviewer

### Which pipeline job do I run?
<details>
<summary> Click to Expand </summary>

#### Stage: snowflake
* **clone_analytics**: Run this when the MR opens to be able to run any dbt jobs. Subsequent runs of this job will be fast as it only verifies if the clone exists.
* **clone_raw**: Run this if you need to run extract, freshness, or snapshot jobs. Subsequent runs of this job will be fast as it only verifies if the clone exists.
* **force_clone_both**: Run this if you want to force refresh both raw and analytics.

#### Stage: extract
* **sheetload**: Run this if you want to test a new sheetload load. This requires the RAW clone to be available.


#### Stage: dbt_run

> As part of a DBT Model Change MR, you need to trigger a pipeline job to test that your changes won't break anything in production. To trigger these jobs, go to the "Pipelines" tab at the bottom of this MR and click on the appropriate stage (dbt_run or dbt_misc).

These jobs are scoped to the `ci` target. This target selects a subset of data for the snowplow and version datasets.

* **all**: Runs all models
* **exclude_product**: Excludes models with the `product` tag. Use this for every other data source.
* **exclude_snowplow**: Excludes just snowplow models.
* **snowplow**: Just runs snowplow data
* **gitlab_dotcom**: Just runs GitLab.com data
* **version**: Just runs usage / version ping data
* **specify_model**: Specify which model to run with the variable `DBT_MODELS`
* **specify_xl_model**: Specify which model to run using an XL warehouse with the variable `DBT_MODELS`
* **specify_exclude**: Specify which model to exclude with the variable `DBT_MODELS`
* **specify_xl_exclude**: Specify which model to exclude using an XL warehouse with the variable `DBT_MODELS`

Watch https://youtu.be/l14N7l-Sco4 to see an example of how to set the variable.

#### Stage: dbt_misc
* **all_tests**: Runs all of the tests
  * Note: it is not necessary to run this job if you've run any of the dbt_run stage jobs as tests are included.
* **data_tests**: Runs only data tests
* **freshness**: Runs source freshness test (requires RAW clone)
* **schema_tests**: Runs only schema tests
* **snapshots**: Runs snapshots (requires RAW clones)
* **specify_tests**: Runs specified model tests with the variable `DBT_MODELS`


#### Stage: python

These jobs only appear when `.py` files have changed. All of them will run automatically on each new commit where `.py` files are present. Otherwise they are unavailable to run.


#### Stage: snowflake_stop

* **clone_stop**: Runs automatically when MR is merged or closed. Do not run manually.

</details>

## Reviewer Checklist
* [ ] Check before setting to merge

## Further changes requested
* [ ] AUTHOR: Uncheck all boxes before taking further action.
