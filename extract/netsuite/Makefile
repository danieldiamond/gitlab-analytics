.PHONY: test run build mypy lint

APP_NAME=netsuite_extractor

build:
	@echo "Building app..."
	docker build -t ${APP_NAME} .

test: build
	@echo "Running full test suite..."

dry_run: build
	@echo "Dry run of netsuite extractor..."
	docker run -e STITCH_CONFIG \
		-e PG_ADDRESS -e PG_DATABASE -e PG_PORT -e PG_USERNAME -e PG_PASSWORD \
  	-e NETSUITE_HOST -e NETSUITE_ENDPOINT -e NETSUITE_EMAIL -e NETSUITE_PASSWORD \
  	-e NETSUITE_ROLE -e NETSUITE_ACCOUNT -e NETSUITE_APPID -e NETSUITE_EARLIEST_DATE \
		${APP_NAME} /bin/bash -c \
		"python netsuite/src/ --schema netsuite export --days 1"

run: build
	@echo "Running netsuite extractor..."
	docker run -e STITCH_CONFIG \
		-e PG_ADDRESS -e PG_DATABASE -e PG_PORT -e PG_USERNAME -e PG_PASSWORD \
  	-e NETSUITE_HOST -e NETSUITE_ENDPOINT -e NETSUITE_EMAIL -e NETSUITE_PASSWORD \
  	-e NETSUITE_ROLE -e NETSUITE_ACCOUNT -e NETSUITE_APPID -e NETSUITE_EARLIEST_DATE \
		-e SNOWFLAKE_LOAD_USER -e SNOWFLAKE_PASSWORD -e SNOWFLAKE_ACCOUNT \
		-e SNOWFLAKE_LOAD_DATABASE -e SNOWFLAKE_LOAD_WAREHOUSE -e SNOWFLAKE_LOAD_ROLE \
		${APP_NAME} /bin/bash -c \
		"python netsuite/src/ --schema netsuite export --days 1 | target-stitch -c netsuite/config.json"

dry_run_types: build
	@echo "Running netsuite transaction_type fetcher..."
	docker run -e STITCH_CONFIG \
		-e PG_ADDRESS -e PG_DATABASE -e PG_PORT -e PG_USERNAME -e PG_PASSWORD \
  	-e NETSUITE_HOST -e NETSUITE_ENDPOINT -e NETSUITE_EMAIL -e NETSUITE_PASSWORD \
  	-e NETSUITE_ROLE -e NETSUITE_ACCOUNT -e NETSUITE_APPID -e NETSUITE_EARLIEST_DATE \
		-e SNOWFLAKE_LOAD_USER -e SNOWFLAKE_PASSWORD -e SNOWFLAKE_ACCOUNT \
		-e SNOWFLAKE_LOAD_DATABASE -e SNOWFLAKE_LOAD_WAREHOUSE -e SNOWFLAKE_LOAD_ROLE \
		${APP_NAME} /bin/bash -c \
		"python netsuite/src/ --schema netsuite extract_type --days 1"

run_types: build
	@echo "Running netsuite transaction_type fetcher..."
	docker run -e STITCH_CONFIG \
		-e PG_ADDRESS -e PG_DATABASE -e PG_PORT -e PG_USERNAME -e PG_PASSWORD \
  	-e NETSUITE_HOST -e NETSUITE_ENDPOINT -e NETSUITE_EMAIL -e NETSUITE_PASSWORD \
  	-e NETSUITE_ROLE -e NETSUITE_ACCOUNT -e NETSUITE_APPID -e NETSUITE_EARLIEST_DATE \
		-e SNOWFLAKE_LOAD_USER -e SNOWFLAKE_PASSWORD -e SNOWFLAKE_ACCOUNT \
		-e SNOWFLAKE_LOAD_DATABASE -e SNOWFLAKE_LOAD_WAREHOUSE -e SNOWFLAKE_LOAD_ROLE \
		${APP_NAME} /bin/bash -c \
		"python netsuite/src/ --schema netsuite extract_type --days 1 | target-stitch -c netsuite/config.json"

run_backlog: build
	@echo "Running netsuite backlog extractor..."
	docker run -e STITCH_CONFIG \
		-e PG_ADDRESS -e PG_DATABASE -e PG_PORT -e PG_USERNAME -e PG_PASSWORD \
  	-e NETSUITE_HOST -e NETSUITE_ENDPOINT -e NETSUITE_EMAIL -e NETSUITE_PASSWORD \
  	-e NETSUITE_ROLE -e NETSUITE_ACCOUNT -e NETSUITE_APPID -e NETSUITE_EARLIEST_DATE \
		-e SNOWFLAKE_LOAD_USER -e SNOWFLAKE_PASSWORD -e SNOWFLAKE_ACCOUNT \
		-e SNOWFLAKE_LOAD_DATABASE -e SNOWFLAKE_LOAD_WAREHOUSE -e SNOWFLAKE_LOAD_ROLE \
		${APP_NAME} /bin/bash -c \
		"python netsuite/src/ --schema netsuite backlog --days 15 | target-stitch -c netsuite/config.json"

dry_run_backlog: build
	@echo "Dry running netsuite backlog extractor..."
	docker run -e STITCH_CONFIG \
		-e PG_ADDRESS -e PG_DATABASE -e PG_PORT -e PG_USERNAME -e PG_PASSWORD \
  	-e NETSUITE_HOST -e NETSUITE_ENDPOINT -e NETSUITE_EMAIL -e NETSUITE_PASSWORD \
  	-e NETSUITE_ROLE -e NETSUITE_ACCOUNT -e NETSUITE_APPID -e NETSUITE_EARLIEST_DATE \
		-e SNOWFLAKE_LOAD_USER -e SNOWFLAKE_PASSWORD -e SNOWFLAKE_ACCOUNT \
		-e SNOWFLAKE_LOAD_DATABASE -e SNOWFLAKE_LOAD_WAREHOUSE -e SNOWFLAKE_LOAD_ROLE \
		${APP_NAME} /bin/bash -c \
		"python netsuite/src/ --schema netsuite backlog --days 15"

mypy: build
	@echo "Running mypy for type-checking..."
	docker run ${APP_NAME} mypy netsuite/src --ignore-missing-imports

lint:
	@echo "Running linter..."
	black src

