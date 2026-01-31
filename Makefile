include .env
export

SPARK_MASTER := spark-master
JOB := /opt/spark/scripts/kafka_to_postgres.py

.PHONY: up up_dev down run stop
up:
	docker compose up -d --build
up_dev:
	docker compose --profile dev up -d --build
down:
	docker compose down --volumes --remove-orphans
stop:
	docker compose stop
run:
	docker compose exec $(SPARK_MASTER) /opt/spark/bin/spark-submit \
		--master spark://$(SPARK_MASTER):7077 \
		$(JOB)

.PHONY: watch_tables
watch_tables:
	watch -n 1 'docker compose exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "SELECT '\''web_logs'\'', count(*) FROM web_logs UNION ALL SELECT '\''processed_data'\'', count(*) FROM processed_data;"'

.PHONY:  open_postgres
open_postgres:
	docker compose exec -it postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
