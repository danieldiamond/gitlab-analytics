.PHONY: build

IMAGE := analytics_webserver_1

help:
	@echo "\n \
	**List of Makefile commands** \n \
	attach: attaches a shell to airflow deployment in docker-compose.yml \n \
	cleanup: deep docker cleanup, frees up space and gets rid of old containers/images \n \
	compose: spins up an airflow deployment in the background and mounts the analytics repo. \n \
	config: generates the docker-compose config to be used in the deployment. \n \
	teardown: stops all running containers and removes them \n"

attach: set-branch compose
	@echo "Attaching to the Webserver container..."
	@docker exec -ti ${IMAGE} /bin/bash

cleanup:
	@echo "Cleaning things up..."
	@docker system prune -f

compose: set-branch
	@echo "Composing airflow..."
	@docker-compose up -d

config: set-branch
	@echo "Generating docker-compose config..."
	@docker-compose config

set-branch:
	@echo "Setting your GIT_BRANCH env var..."
	@export GIT_BRANCH=$(git symbolic-ref --short HEAD)
	@echo "Your GIT_BRANCH is now set to ${GIT_BRANCH}"

teardown:
	@echo "Tearing down docker containers..."
	@docker-compose down
