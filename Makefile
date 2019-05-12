.PHONY: build

IMAGE := analytics_webserver_1

help:
	@echo "\n \
	**List of Makefile commands** \n \
	attach: attaches a shell to airflow deployment in docker-compose.yml. \n \
	cleanup: WARNING: DELETES DB VOLUME, frees up space and gets rid of old containers/images. \n \
	compose: spins up an airflow deployment in the background and mounts the analytics repo. \n \
	init: initializes a new Airflow db, required on a fresh db. \n \
	lint-dags: runs a linter against the dags dir. \n \
	set-branch: prints a command for the user to run that will set their GIT_BRANCH env var. \n"

attach: compose
	@echo "Attaching to the Webserver container..."
	@sleep 5
	@docker exec -ti ${IMAGE} /bin/bash

cleanup:
	@echo "Cleaning things up..."
	@docker-compose down -v
	@docker system prune -f

compose:
	@echo "Composing airflow..."
	@docker-compose up -d

init:
	@echo "Initializing the Airflow DB..."
	@docker-compose up -d db
	@sleep 5
	@docker-compose run scheduler airflow initdb
	@docker-compose down

set-branch:
	@echo "Run this command to properly set your GIT_BRANCH env var:"
	@echo "export GIT_BRANCH=$$(git symbolic-ref --short HEAD)"

lint-dags:
	@echo lintings the dags/ dir...
	@black dags/

lint-orchestration:
	@echo linting the orchestration scripts...
	@black orchestration/
