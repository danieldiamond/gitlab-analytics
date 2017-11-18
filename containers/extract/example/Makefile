stop:
	@docker-compose down

run-carte-master:
	@docker-compose up master

run-carte-slave:
	@docker-compose up slave

run-kitchen:
	@docker-compose run master kitchen.sh -rep=docker-pentaho-di -dir=/ -job=test-job
