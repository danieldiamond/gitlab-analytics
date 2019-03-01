.PHONY: build

IMAGE := airflow-image:test

help:
	@echo "\n \
	**These are the supported commands** \n \
	compose: spins up an airflow-deployment and mounts the analytics repo. \n"

compose:
	@echo "Composing airflow..."
	@docker-compose up

attach:
	@echo "Attaching to the running Webserver container..."
	@docker exec -ti something

cleanup:
	@echo "Cleaning things up..."
	@docker system prune -f
