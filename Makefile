.PHONY: build

AIRFLOW_IMAGE := analytics_airflow_webserver_1

help:
	@echo "\n \
	------------------------------ \n \
	++ Airflow Related ++ \n \
	airflow: attaches a shell to the airflow deployment in docker-compose.yml. \n \
	init-airflow: initializes a new Airflow db, required on a fresh db. \n \
	lint-dags: runs a linter against the dags dir. \n \
	\n \
	++ dbt Related ++ \n \
	data-image: attaches to a shell in the data-image and mounts the local dbt repo for testing. \n \
	\n \
	++ Utilities ++ \n \
	cleanup: WARNING: DELETES DB VOLUME, frees up space and gets rid of old containers/images. \n \
	lint-orchestration: runs a linter against the orchestration dir. \n \
	set-branch: prints a command for the user to run that will set their GIT_BRANCH env var. \n \
	------------------------------ \n"

airflow:
	@echo "Attaching to the Webserver container..."
	@docker-compose down
	@docker-compose up -d airflow_webserver
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
	@docker-compose down

lint-dags:
	@echo lintings the dags/ dir...
	@black dags/

lint-orchestration:
	@echo linting the orchestration scripts...
	@black orchestration/

set-branch:
	@echo "Run this command to properly set your GIT_BRANCH env var:"
	@echo "export GIT_BRANCH=$$(git symbolic-ref --short HEAD)"
