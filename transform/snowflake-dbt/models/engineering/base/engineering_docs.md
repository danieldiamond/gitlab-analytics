{% docs engineering_advisory_data %}

The data is derived from files contained in [gemnasium-db repository](https://gitlab.com/gitlab-org/security-products/gemnasium-db). The CI job leverages Git metadata and filesystem information directly and computes the following information:

* full file path from advisory file
* git merge date for advisory file
* relevant data fields from advisory file (file has to be parsed)
* package type to which the advisory relates

The script that performs these steps is available [here](https://gitlab.com/gitlab-org/security-products/gemnasium-db/blob/master/stats/scripts/prepare_data.rb).

The data is available via [this link](https://gitlab.com/gitlab-org/security-products/gemnasium-db/-/jobs/artifacts/master/raw/data/data.tar.gz?job=pages).

{% enddocs %}


{% docs engineering_nvd_data %}

The data is derived from the National Vulnerability Database and details the coverage. 

The script that performs these steps is available [here](https://gitlab.com/gitlab-org/security-products/gemnasium-db/blob/master/stats/scripts/prepare_data.rb).

The data is available via [this link](https://gitlab.com/gitlab-org/security-products/gemnasium-db/-/jobs/artifacts/master/raw/data/nvd.tar.gz?job=pages).

There are no columns on the csv, but the first column is year and the second is count.

{% enddocs %}
