## Issue
<!---
Link the Issue this MR closes
--->
Closes #

## Solution

Describe the solution.

### Related Links

Please include links to any related MRs and/or issues.

## Submitter Checklist

- [ ] This MR follows the coding conventions laid out in the [style guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/)

#### Structure
- [ ] Model-specific attributes (like custom schemas and materializations) should be specified in the model
- [ ] Only base models are used to reference source tables/views
- [ ] All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file

#### Style
- [ ] Field names should all be lowercased
- [ ] Function names should all be capitalized
- [ ] This MR contains new macros
  - [ ] New macros follow the naming convention (file name matches macro name) 
  - [ ] New macros have been documented in the macro README

#### Testing
- [ ] Every model should be tested AND documented in a `schema.yml` file. At minimum, unique, not nullable fields, and foreign key constraints should be tested, if applicable ([Docs](https://docs.getdbt.com/docs/testing-and-documentation))
- [ ] The output of dbt test should be pasted into MRs directly below this point

<details>
<summary> dbt test results </summary>

<pre><code>

Paste the results of dbt test here, including the command.

</code></pre>
</details>

## All MRs Checklist
* [ ]  [Label hygiene](https://about.gitlab.com/handbook/business-ops/data-team/#issue-labeling) on issue
* [ ]  Pipelines pass
* [ ]  This MR is ready for final review and merge.
* [ ]  Assigned to reviewer

<details>
<summary> Which job do I run? </summary>

> As part of a DBT Model Change MR, you need to trigger a pipeline job to test that your changes won't break anything in production. To trigger these jobs, go to the "Pipelines" tab at the bottom of this MR and click on the appropriate stage (model or model_tests).

These jobs are scoped to the `ci` target. This target selects a subset of data for the snowplow and pings datasets.

Stage: model
* **mr_dbt_archive_manual**: For changes to dbt archive
* **mr_dbt_all**: Runs all models
* **mr_dbt_exclude_product**: Excludes models with the `product` tag. Use this for every other data source.
* **mr_dbt_snowplow**: Just runs snowplow data
* **mr_dbt_gitlab_dotcom**: Just runs GitLab.com data
* **mr_dbt_pings**: Just runs usage / version ping data

Stage: model_tests
* **mr_dbt_tests_manual**: Runs all of the tests
  * Note: it is not necessary to run this job if you've run any of the model stage jobs as tests are included.

</details>

## Reviewer Checklist
* [ ]  Check before setting to merge

## Further changes requested
* [ ]  AUTHOR: Uncheck all boxes before taking further action. 
