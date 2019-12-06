Closes

* List the tables added/changed below
* Run the `clone_raw` CI job
* Run the `pgp_test` CI job by right clicking on the job name and opening in a new tab
  * Include the `MANIFEST_NAME` variable and input the name of the db (i.e. `gitlab_com`, `customers`, etc.)
  * If this is a SCD table be sure to include:
    * `advanced_metadata: true` in the manifest
    * `TASK_INSTANCE` variable in job trigger with any value (i.e. `mr-2112`)

#### Tables Changed/Added

* [ ] List

#### PGP Test CI job passed?

* [ ] List
