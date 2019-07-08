### Team.yml Extractor

This job pulls an Airflow generated JSON file into a table in Snowflake called raw.team_yaml.team_yaml. It uses [remarshal](https://pypi.org/project/remarshal/) to serialize YAML to JSON. This is done in the team_yaml.py DAG in the dags/extract directory.