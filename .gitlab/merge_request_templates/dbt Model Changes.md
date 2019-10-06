## Issue
<!---
Link the Issue this MR closes
--->
Closes #

## Solution

Describe the solution.

### Related Links

Please include links to any related MRs and/or issues.

## Stakeholder Checklist

If you are the person who will be using this data and/or the dashboard it depends on, please fill out this section.

- [ ] Does the dbt model change provide the requested data? 
- [ ] Does the dbt model change provide accurate data?

## Submitter Checklist

Please go through every line

- [ ] This MR follows the coding conventions laid out in the [SQL style guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/), including the [dbt guidelines](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/#dbt-guidelines).

#### What are you using to audit your results are accurate?

If you have an existing report/dashboard/dataset as reference, please provide your query used to validate the results of your model changes. If this is the first iteration of a model or validation is otherwise out of scope, please provide additional context.

<details>
<summary> Paste query and results here </summary>

<pre><code>

Example: You might be looking at the count of opportunities before and after, if you're editing the opportunity model.

</code></pre>
</details>

#### Structure

- [ ] Ensure source tables/views are only referenced within [base models](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/#base-models).
- [ ] All references to existing tables/views/sources (i.e. `{{ ref('...') }}` statements) should be placed in CTEs at the top of the file.
- [ ] If you are using [custom schemas](https://docs.getdbt.com/docs/using-custom-schemas) or modifying [materializations](https://docs.getdbt.com/docs/materializations), ensure these attributes are specified in the model.

#### Style

- [ ] Field names should all be lowercased.
- [ ] Function names should all be capitalized.

#### Macros

- [ ] Does this MR utilize [macros](https://docs.getdbt.com/docs/macros)?
  - [ ] This MR contains new macros. Follow the naming convention (file name matches macro name) and document in the [macro README](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/README.md).
  - [ ] This MR uses existing macros. Ensure models are referenced under the appropriate macro in the [macro README](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/README.md).

#### Incremental Models

- [ ] Does this MR contain an [incremental model](https://docs.getdbt.com/docs/configuring-incremental-models#section-how-do-i-use-the-incremental-materialization-)?
  - [ ] If the MR adds/renames columns to a specific model, a `dbt run --full-refresh` will be needed after merging the MR. Please, add it to the Reviewer Checklist to warn them that this step is required.
  - [ ] Please also check with the Reviewer if a dag is set up in Airflow to trigger a full refresh of this model.  

#### Testing

- [ ] Every model should be [tested](https://docs.getdbt.com/docs/testing-and-documentation) AND documented in a `schema.yml` file. At minimum, unique, not nullable fields, and foreign key constraints should be tested, if applicable.
- [ ] Please paste the output of dbt test from your local environment below. Any failing tests should be fixed or explained prior to requesting a review.

<details>
<summary> dbt test results </summary>

<pre><code>

Paste the results of dbt test here, including the command.

</code></pre>
</details>

- [ ] Run the appropriate pipeline for the model changes in this MR
<details>
<summary> Which pipeline job do I run? </summary>

#### Stage: snowflake

- **clone_analytics**: Run this when the MR opens to be able to run any dbt jobs. Subsequent runs of this job will be fast as it only verifies if the clone exists.
- **clone_raw**: Run this if you need to run extract, freshness, or snapshot jobs. Subsequent runs of this job will be fast as it only verifies if the clone exists.
- **force_clone_both**: Run this if you want to force refresh both raw and analytics.

#### Stage: extract

- **sheetload**: Run this if you want to test a new sheetload load. This requires the RAW clone to be available.


#### Stage: dbt_run

> As part of a DBT Model Change MR, you need to trigger a pipeline job to test that your changes won't break anything in production. To trigger these jobs, go to the "Pipelines" tab at the bottom of this MR and click on the appropriate stage (dbt_run or dbt_misc).

These jobs are scoped to the `ci` target. This target selects a subset of data for the snowplow and pings datasets.

- **all**: Runs all models
- **exclude_product**: Excludes models with the `product` tag. Use this for every other data source.
- **gitlab_dotcom**: Just runs GitLab.com models
- **pings**: Just runs usage / version ping models
- **snowplow**: Just runs snowplow and snowplow_combined models
- **specify_model**: Specify which model to run with the variable `DBT_MODELS`
- **specify_xl_model**: Specify which model to run using an XL warehouse with the variable `DBT_MODELS`
- **specify_exclude**: Specify which model to exclude with the variable `DBT_MODELS`
- **specify_xl_exclude**: Specify which model to exclude using an XL warehouse with the variable `DBT_MODELS`

Watch https://youtu.be/l14N7l-Sco4 to see an example of how to set the variable.

#### Stage: dbt_misc

- **all_tests**: Runs all of the tests
  - Note: it is not necessary to run this job if you've run any of the dbt_run stage jobs as tests are included.
- **data_tests**: Runs only data tests
- **freshness**: Runs source freshness test (requires RAW clone)
- **schema_tests**: Runs only schema tests
- **snapshots**: Runs snapshots (requires RAW clones)
- **specify_tests**: Runs specified model tests with the variable `DBT_MODELS`


#### Stage: python

These jobs only appear when `.py` files have changed. All of them will run automatically on each new commit where `.py` files are present. Otherwise they are unavailable to run.


#### Stage: snowflake_stop

- **clone_stop**: Runs automatically when MR is merged or closed. Do not run manually.

</details>

## All MRs Checklist
- [ ] [Label hygiene](https://about.gitlab.com/handbook/business-ops/data-team/#issue-labeling) on issue
- [ ] Pipelines pass
- [ ] Branch set to delete
- [ ] Commits NOT set to squash
- [ ] This MR is ready for final review and merge.
- [ ] Resolve all threads
- [ ] Remove the `WIP:` prefix in the MR title before assigning to reviewer
- [ ] Assigned to reviewer

## Reviewer Checklist
- [ ]  Check before setting to merge

## Further changes requested
- [ ]  AUTHOR: Uncheck all boxes before taking further action.
