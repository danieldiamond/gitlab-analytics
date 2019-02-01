.PHONY: test run build mypy lint

APP_NAME=tap-postgres

build:
	@echo "Building app..."
	docker build -t ${APP_NAME} .

test: build
	@echo "Running full test suite..."

run_ci_stats: build
	@echo "Running tap-postgres..."
	docker run -e CI_STATS_DB_USER -e CI_STATS_DB_PASS -e CI_STATS_DB_HOST \
		-e CI_STATS_DB_NAME -e PG_PORT -e HOURS=8 ${APP_NAME} \
		python tap_postgres/tap_postgres.py tap manifests/ci_stats_db_manifest.yaml

mypy: build
	@echo "Running mypy for type-checking..."
	docker run ${APP_NAME} mypy tap_postgres --ignore-missing-imports

lint:
	@echo "Running linter..."
	black tap_postgres tests

