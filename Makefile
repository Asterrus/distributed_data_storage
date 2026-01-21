SPARK_MASTER := spark-master
JOB := /opt/spark/scripts/python_task.py

.PHONY: up down run
up:
	docker compose up -d --build

down:
	docker compose down --volumes

run:
	docker compose exec $(SPARK_MASTER) /opt/spark/bin/spark-submit \
		--master spark://$(SPARK_MASTER):7077 \
		$(JOB)
