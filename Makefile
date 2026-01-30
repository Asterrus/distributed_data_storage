include .env
export

SPARK_MASTER := spark-master
JOB := /opt/spark/scripts/python_task.py

.PHONY: up down run 
up:
	docker compose up -d --build

down:
	docker compose down --volumes --remove-orphans

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
