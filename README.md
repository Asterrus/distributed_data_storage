### Описание
Работа с s3 хранилищем.Parquet. Iceberg.  

### Инструменты

- docker
- docker-compose
- uv
- PostgreSQL
- MinIO

### Запуск

1. Клонировать репозиторий и перейти в директорию проекта:

   ```bash
   git clone ...
   cd distributed_data_storage
   ```

2. Создать .env файл с переменными окружения:

   ```bash
   cp .env.example .env
   ```

3. Запустить docker-compose.  
   Таблица логов создается из файла scripts/db/init.sql.  
   Хранилище s3 связывается с папкой data в корне проекта.  
   Адрес s3: http://localhost:9001  
   Логин: minio Пароль: minio123.  
   Есть в .env.example(MINIO_ROOT_USER и MINIO_ROOT_PASSWORD). 
   Создается bucket "logs-bucket".  

   ```bash
   docker compose up -d
   ```

4. Основной скрипт. генерация данных в Postgres.  
   Загрузка данных из Postgres в s3 в формате parquet и iceberg:  

   ```bash
   uv run main.py
   ```

Сравнение целевых данных в формате Parquet и Iceberg:  
Путь к bucket:  
```
data/minio/logs-bucket
```
Parquet формат содержит файлы с данными(part.*), и мета данные этих файлов(xl.meta).  

Iceberg формат добавляет к Parquet файлам(data) слой метаданных(metadata) для обеспечения ACID, версионирования, хранения схем таблиц, безопасной конкурентной записи.

Слой metadata Iceberg:
1. UUID.metadata.json файлы - Метаинформация таблицы, для разных версий таблицы
2. UUID.avro - манифест файлы, реестр всех файлов Parquet
3. snap-UUID.avro - Snapshot файлы, снимки состояний таблицы в разное время
