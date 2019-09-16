### YML Extractor

This job pulls an Airflow-generated JSON files into tables in Snowflake within the raw.gitlab_data_yaml schema. It uses [remarshal](https://pypi.org/project/remarshal/) to serialize YAML to JSON. This is done in the gitlab_data_yaml.py DAG in the dags/extract directory.

Current files are:

* [Location Factors](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/location_factors.yml)
* [Roles](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/roles.yml)
* [Team](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/team.yml)