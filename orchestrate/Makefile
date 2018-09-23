.PHONY: test test-quick

test:
	@echo "Full test run starting..."
	@time docker-compose run orchestrator pytest -vv

test-quick:
	@echo "Running non-integration tests..."
	@time docker-compose run orchestrator \
		pytest /orchestrate/tests/test_ci_response_helpers.py \
					 /orchestrate/tests/test_config_reader.py \
