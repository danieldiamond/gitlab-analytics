The current process for the gitlab.com extract:

* There is a [pseudonymizer worker](https://gitlab.com/gitlab-org/gitlab-ee/blob/master/ee/app/workers/pseudonymizer_worker.rb) that will run:
  * The [dumper.rb](https://gitlab.com/gitlab-org/gitlab-ee/blob/master/ee/lib/pseudonymizer/dumper.rb), [uploader.rb](https://gitlab.com/gitlab-org/gitlab-ee/blob/master/ee/lib/pseudonymizer/uploader.rb), and other minor jobs
  * Pseudonymization is based on a [YAML file](https://gitlab.com/gitlab-org/gitlab-ee/blob/master/config/pseudonymizer.yml).
  * The extract is pointing at the geo database.
* This is configured to upload to a Google Cloud Storage bucket.
* On the analytics side, we [have a job](https://gitlab.com/meltano/analytics/blob/master/extract/gitlab/src/__main__.py) to fetch these CSVs and upload to PostgreSQL


----

Improvements that are necessary:

* Consistently run export job for GitLab (once / week)
* Disable pseudonymization
* Make the extraction incremental so it's not full table exports

Alternatively, we could convert this to taps and targets via:

* Postgres tap to CSV target followed by CSV tap to Snowflake Target
* Postgres tap to Snowflake target