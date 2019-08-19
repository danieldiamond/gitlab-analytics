.PHONY: build

AIRFLOW_IMAGE := analytics_airflow_webserver_1
GIT_BRANCH := $$(git symbolic-ref --short HEAD)

help:
	@echo "\n \
	------------------------------ \n \
	++ Airflow Related ++ \n \
	airflow: attaches a shell to the airflow deployment in docker-compose.yml. \n \
	init-airflow: initializes a new Airflow db, required on a fresh db. \n \
	\n \
	++ Python/dbt Related ++ \n \
	data-image: attaches to a shell in the data-image and mounts the local dbt repo for testing. \n \
	\n \
	++ Utilities ++ \n \
	cleanup: WARNING: DELETES DB VOLUME, frees up space and gets rid of old containers/images. \n \
	lint: Runs a linter (Black) over the whole repo. \n \
	pylint: Runs the pylint checker over the whole repo. Does not check for code formatting, only errors/warnings. \n \
	radon: Runs a cyclomatic complexity checker and shows anything with less than an A rating. \n \
	xenon: Runs a cyclomatic complexity checker that will throw a non-zero exit code if the criteria aren't met. \n \
	------------------------------ \n"

airflow:
	@echo "Attaching to the Webserver container..."
	@docker-compose down
	@export GIT_BRANCH=$(GIT_BRANCH) && docker-compose up -d airflow_webserver
	@sleep 5
	@docker exec -ti ${AIRFLOW_IMAGE} /bin/bash

cleanup:
	@echo "Cleaning things up..."
	@docker-compose down -v
	@docker system prune -f

data-image:
	@echo "Attaching to data-image and mounting repo..."
	@docker-compose run data_image bash

init-airflow:
	@echo "Initializing the Airflow DB..."
	@docker-compose up -d airflow_db
	@sleep 5
	@docker-compose run airflow_scheduler airflow initdb
	@docker-compose run airflow_scheduler airflow create_user --role Admin -u admin -p admin -e datateam@gitlab.com -f admin -l admin
	@docker-compose down

lint:
	@echo "Linting the repo..."
	@black .

pylint:
	@echo "Running pylint..."
	@pylint ../analytics/ --ignore=dags --disable=C --disable=W1203 --disable=W1202 --reports=y

radon:
	@echo "Run Radon to compute complexity..."
	@radon cc . --total-average -nb

xenon:
	@echo "Running Xenon..."
	@xenon --max-absolute B --max-modules A --max-average A . -i transform,shared_modules
