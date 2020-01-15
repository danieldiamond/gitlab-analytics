## Issue
<!--- Link the Issue this MR closes --->
Closes #

## Solution

Describe the solution. Include links to any related MRs and/or issues.

## Stakeholder Checklist
<details>
<summary><i>Click to toggle Stakeholder Checklist</i></summary>
If you are the person who will be using this data and/or the dashboard it depends on, please fill out this section.

- [ ] Does the dbt model change provide the requested data? 
- [ ] Does the dbt model change provide accurate data?
</details>

## Submitter Checklist

#### Style & Structure
<details>
<summary><i>Click to toggle Style & Structure</i></summary>

- [ ] Field names should all be lowercased.
- [ ] Function names should all be capitalized.
- [ ] Ensure source tables/views are only referenced within [base models](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/#base-models).
- [ ] All references to existing tables/views/sources (i.e. `{{ ref('...') }}` statements) should be placed in CTEs at the top of the file.
- [ ] If you are using [custom schemas](https://docs.getdbt.com/docs/using-custom-schemas) or modifying [materializations](https://docs.getdbt.com/docs/materializations), ensure these attributes are specified in the model.
</details>

#### Macros

- [ ] Does this MR utilize [macros](https://docs.getdbt.com/docs/macros)?
  - [ ] This MR contains new macros. Follow the naming convention (file name matches macro name) and document in the [macro README](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/README.md).
  - [ ] This MR uses existing macros. Ensure models are referenced under the appropriate macro in the [macro README](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/README.md).

#### Incremental Models

- [ ] Does this MR contain an [incremental model](https://docs.getdbt.com/docs/configuring-incremental-models#section-how-do-i-use-the-incremental-materialization-)?
  - [ ] If the MR adds/renames columns to a specific model, a `dbt run --full-refresh` will be needed after merging the MR. Please, add it to the Reviewer Checklist to warn them that this step is required.
  - [ ] Please also check with the Reviewer if a dag is set up in Airflow to trigger a full refresh of this model.  

#### Schema or Model Name Changes
- [ ] Does this MR change the **schema** or **model name** of any existing models?
  - [ ] Create an issue to change all existing periscope reporting to reference the new schema/name.
  - [ ] After merging, ensure the old model is dropped from snowflake. This can be done by creating an issue specifying the tables/models to be dropped and assiging to a snowflake admin. 

#### What are you using to audit your results are accurate?

If you have an existing report/dashboard/dataset as reference, please provide your query used to validate the results of your model changes. If this is the first iteration of a model or validation is otherwise out of scope, please provide additional context.

<details>
<summary> Paste query and results here </summary>

<pre><code>

Example: You might be looking at the count of opportunities before and after, if you're editing the opportunity model.

</code></pre>
</details>

#### Testing

- [ ] Every model should be [tested](https://docs.getdbt.com/docs/testing-and-documentation) AND documented in a `schema.yml` file. At minimum, unique, not nullable fields, and foreign key constraints should be tested, if applicable.
- [ ] Run the appropriate pipeline for the model changes in this MR
- [ ] If the periscope_query job failed, validate that the changes you've made don't affect the grain of the table or the expected output in Periscope.

<details>
<summary> Which pipeline job do I run? </summary>

#### Stage: snowflake

- **clone_analytics**: Runs automatically when the MR opens to be able to run any dbt jobs. Subsequent runs of this job will be fast as it only verifies if the clone exists. This is an empty clone of the analytics db.
- **clone_analytics_real**: Run this if you need to do a real clone of the analytics warehouse. This is a full clone of the db.
- **clone_raw**: Run this if you need to run extract, freshness, or snapshot jobs. Subsequent runs of this job will be fast as it only verifies if the clone exists.
- **force_clone_both**: Run this if you want to force refresh both raw and analytics.

#### Stage: extract

- **boneyard_sheetload**: Run this if you want to test a new boneyard sheetload load. This requires the real analytics clone to be available.
- **sheetload**: Run this if you want to test a new sheetload load. This requires the RAW clone to be available.
- **pgp_test**: Run this if you're adding or updating a postgres pipeline manifest. Requires MANIFEST_NAME variable, possibly TASK_INSTANCE variable, and the RAW clone to be available. 


#### Stage: dbt_run

> As part of a DBT Model Change MR, you need to trigger a pipeline job to test that your changes won't break anything in production. To trigger these jobs, go to the "Pipelines" tab at the bottom of this MR and click on the appropriate stage (dbt_run or dbt_misc).

These jobs are scoped to the `ci` target. This target selects a subset of data for the snowplow and version datasets.

Note that job artificats are available for all dbt run jobs. These include the compiled code and the run results.

- **specify_model**: Specify which model to run with the variable `DBT_MODELS`
- **specify_xl_model**: Specify which model to run using an XL warehouse with the variable `DBT_MODELS`
- **specify_exclude**: Specify which model to exclude with the variable `DBT_MODELS`
- **specify_xl_exclude**: Specify which model to exclude using an XL warehouse with the variable `DBT_MODELS`

Watch https://youtu.be/l14N7l-Sco4 to see an example of how to set the variable. The variable is a stand-in for any of the examples in [the dbt documentation on model selection syntax](https://docs.getdbt.com/docs/model-selection-syntax#section-specifying-models-to-run).

#### Stage: dbt_misc
* **all_tests**: Runs all of the tests
  * Note: it is not necessary to run this job if you've run any of the dbt_run stage jobs as tests are included.
* **data_tests**: Runs only data tests
* **freshness**: Runs source freshness test (requires RAW clone)
* **periscope_query**: Runs automatically. See documentation below
* **schema_tests**: Runs only schema tests
* **snapshots**: Runs snapshots (requires RAW clones)
* **specify_tests**: Runs specified model tests with the variable `DBT_MODELS`

##### Job: Periscope Query

This job runs automatically and only appears when `.sql` files are changed. In its simplest form, the job will check to see if any of the currently changed models are queried in Periscope. If they are, the job will fail with a notification to check the relevant dashboard. If it is not queried, the job will succeed.

Current caveats with the job are:

* It will not tell you which dashboard to check
* It is not able to validate tables that are queried with any string interpolation syntax (i.e. `retention_[some_variable]`)
* It is not able to validate if a table is aliased via dbt

For more details on the bash commands, see the expandle bash details section at the end of the MR description.

#### Stage: python

These jobs only appear when `.py` files have changed. All of them will run automatically on each new commit where `.py` files are present. Otherwise they are unavailable to run.


#### Stage: snowflake_stop

- **clone_stop**: Runs automatically when MR is merged or closed. Do not run manually.

</details>

- [ ] Please paste the output of dbt test below. Any failing tests should be fixed or explained prior to requesting a review.

<details>
<summary> dbt test results </summary>

<pre><code>

Paste the results of dbt test here, including the command.

</code></pre>
</details>

## All MRs Checklist
- [ ] This MR follows the coding conventions laid out in the [SQL style guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/), including the [dbt guidelines](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/#dbt-guidelines).
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
* [ ]  AUTHOR: Uncheck all boxes before taking further action.

<details>
<summary>Periscope Query Bash Details</summary>

`git clone -b periscope/master --single-branch https://gitlab.com/gitlab-data/periscope.git --depth 1`

This clones the periscope project.

`grep -rIiEo "from (analytics|analytics_staging|boneyard)\.([\_A-z]*)" periscope/. | awk -F '.' '{print tolower($NF)}' | sort | uniq > periscope.txt`

This recursively searches the entire git repo for a string that matches a `from` statement from any of the 3 currently queryable schemas. Using `awk`, it then prints the lower-case of the last column of each line in a file (represented by $NF - which is the number of fields), using a period as a field separator. This works because all queries are some form of <schema>.<table> and what we want is the table. It then sorts the results, gets the unique set, and writes it to a file called periscope.txt.

`git diff origin/$CI_MERGE_REQUEST_TARGET_BRANCH_NAME...HEAD --name-only | grep -iEo "(.*)\.sql" | sed -E 's/\.sql//' | awk -F '/' '{print tolower($NF)}' | sort | uniq > diff.txt`

This gets the list of files that have changed from the master branch (i.e. target branch) to the current commit (HEAD). It then finds (grep) only the sql files and substitutes (sed) the `.sql` with an empty string. Using `awk`, it then prints the lower-case of the last column of each line in a file (represented by $NF - which is the number of fields), using a slash (/) as a field separator. Since the output is directoy/directoy/filename and we make the assumption that most dbt models will write to a table named after its file name, this works as expected. It then sorts the results, gets the unique set, and writes it to a file called diff.txt.

`comm -12 periscope.txt diff.txt > comparison.txt`

This compares (comm) two files and print only lines that are common to both files. It saves it to a file called comparison.txt

`if (( $(cat comparison.txt | wc -l | tr -d ' ') > 0 )); then echo "Check these!" && cat comparison.txt && exit 1; else echo "All good" && exit 0; fi;`

This uses word count (wc) to see how many lines are in the comparison file. If there is more than zero it will print the lines and exit with a failure. If there are no lines it exits with a success.
</details>
