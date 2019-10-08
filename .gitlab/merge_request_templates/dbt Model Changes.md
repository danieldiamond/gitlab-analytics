## Issue
<!---
Link the Issue this MR closes
--->
Closes #

## Solution

Describe the solution.

## What number/query are you using to audit your results are accurate?

<!---
Example: You might be looking at the count of opportunities before and after, if you're editing the opportunity model.
--->

### Related Links

Please include links to any related MRs and/or issues.

## Stakeholder Checklist (if applicable)

- [ ] Does the dbt model change provide the requested data?
- [ ] Does the dbt model change provide accurate data?

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
- [ ] This MR uses existing macros. Reference models using these macros [here](https://gitlab.com/gitlab-data/analytics/blob/73751832a5415389b60d41ef92ee8deaef374734/transform/snowflake-dbt/macros/README.md)

#### Testing
- [ ] Every model should be tested AND documented in a `schema.yml` file. At minimum, unique, not nullable fields, and foreign key constraints should be tested, if applicable ([Docs](https://docs.getdbt.com/docs/testing-and-documentation))
- [ ] The output of dbt test should be pasted into MRs directly below this point

<details>
<summary> dbt test results </summary>

<pre><code>

Paste the results of dbt test here, including the command.

</code></pre>
</details>

- [ ] If the periscope_query job failed, validate that the changes you've made don't affect the grain of the table or the expected output in Periscope.

#### If you use incremental dbt models
* [ ] If the MR adds/renames columns to a specific model, a `dbt run --full-refresh` will be needed after merging the MR. Please, add it to the Reviewer Checklist to warn them that this step is required.
* [ ] Please also check with the Reviewer if a dag is set up in Airflow to trigger a full refresh of this model.

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

These jobs are scoped to the `ci` target. This target selects a subset of data for the snowplow and pings datasets.

* **all**: Runs all models
* **exclude_product**: Excludes models with the `product` tag. Use this for every other data source.
* **gitlab_dotcom**: Just runs GitLab.com models
* **pings**: Just runs usage / version ping models
* **snowplow**: Just runs snowplow and snowplow_combined models
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

* **clone_stop**: Runs automatically when MR is merged or closed. Do not run manually.

</details>

## Reviewer Checklist
* [ ]  Check before setting to merge

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
