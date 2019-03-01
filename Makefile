.PHONY: build

IMAGE := analytics_webserver_1

help:
	@echo "\n \
	**List of Makefile commands** \n \
	attach: attaches a shell to airflow deployment in docker-compose.yml \n \
	cleanup: deep docker cleanup, frees up space and gets rid of old containers/images \n \
	compose: spins up an airflow deployment in the background and mounts the analytics repo. \n \
	teardown: stops all running containers and removes them \n"

compose:
	@echo "Composing airflow..."
	@docker-compose up -d

teardown:
	@echo "Tearing down docker containers..."
	@docker-compose down

attach: compose
	@echo "Attaching to the Webserver container..."
	@docker exec -ti ${IMAGE} /bin/bash

cleanup:
	@echo "Cleaning things up..."
	@docker system prune -f
