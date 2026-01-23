### Описание
Работа с s3 хранилищем.Postgres -> S3 -> Postgres.  

### Инструменты

- docker
- docker-compose
- uv
- PostgreSQL
- MinIO
- Apache Spark

### Запуск

1. Клонировать репозиторий и перейти в директорию проекта:

   ```bash
   git clone ...
   cd distributed_data_storage
   ```

2. Перейти на ветку big_data_apache_spark
   ```bash
   git checkout distributed_data_storage
   ```

3. Создать .env файл с переменными окружения:

   ```bash
   cp .env.example .env
   ```

4. Запустить docker-compose.  
   Таблица логов создается из файла scripts/db/init.sql.  
   Хранилище s3 связывается с папкой data в корне проекта.  
   Адрес s3: http://localhost:9001  
   Логин: minio Пароль: minio123.  
   Есть в .env.example(MINIO_ROOT_USER и MINIO_ROOT_PASSWORD). 
   Создается bucket "logs-bucket".  

   Поднимается Apache Spark(Мастер+Воркер)
   ```bash
   make up
   ```
   или
   ```bash
   docker compose up -d
   ```

5. Генерация данных в Postgres.  
   Загрузка данных из Postgres в s3 в формате parquet и iceberg:  

   ```bash
   uv run main.py
   ```

6. Выполнение скрипта python, S3 -> PostgreSQL, через spark-submit Мастер-контейнера Spark:  

   ```bash
   make run
   ```
   или
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/scripts/python_task.py
   ```

7. Выполнение скрипта python, S3 -> PostgreSQL, через Polars(Без параллельной записи в БД):  
   ```bash
   uv run s3_to_postgres.py
   ```

Выводы:
Pyspark позволяет задать количество партиций, размер батчей, берёт на себя параллельную и распределённую обработку данных, позволяет писать более декларативный код.
На Python пришлось бы самим реализовывать обработку на разных процессах, синхронизировать всё между собой.
Spark тяжелый, много зависимостей, требует настройки инфраструктуры.
Для небольших объемов данных - Python. Для больших - Spark.
