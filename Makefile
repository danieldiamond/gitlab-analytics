.PHONY: build

IMAGE := analytics_webserver_1

help:
	@echo "\n \
	**List of Makefile commands** \n \
	attach: attaches a shell to airflow deployment in docker-compose.yml. \n \
	cleanup: deep docker cleanup, frees up space and gets rid of old containers/images. \n \
	compose: spins up an airflow deployment in the background and mounts the analytics repo. \n \
	config: generates the docker-compose config to be used in the deployment. \n \
	init: initializes a new Airflow db, required on a fresh db. \n \
	teardown: stops all running containers and removes them. \n"

attach: compose
	@echo "Attaching to the Webserver container..."
	@docker exec -ti ${IMAGE} /bin/bash

cleanup:
	@echo "Cleaning things up..."
	@docker system prune -f

compose:
	@echo "Composing airflow..."
	@docker-compose up -d

config:
	@echo "Generating docker-compose config..."
	@docker-compose config

init:
	@echo "Initializing the Airflow DB..."
	@docker-compose up -d db
	@docker-compose run scheduler airflow initdb
	@docker-compose down

set-branch:
	@echo "Run this command to properly set your GIT_BRANCH env var:"
	@echo "export GIT_BRANCH=$$(git symbolic-ref --short HEAD)"

teardown:
	@echo "Tearing down docker containers..."
	@docker-compose down
