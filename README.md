### Описание
Работа с s3 хранилищем.Parquet. Iceberg.  

### Инструменты

- docker
- docker-compose
- uv

### Запуск

1. Клонировать репозиторий и перейти в директорию проекта:

   ```bash
   git clone ...
   cd distributed_data_storage
   ```

2. Запустить docker-compose.  
   Таблица логов создается из файла scripts/db/init.sql.  
   Хранилище s3 связывается с папкой data в корне проекта.  
   Создается bucket "logs-bucket" в хранилище s3.  

   ```bash
   docker compose up -d
   ```

3. Основной скрипт. генерация данных в Postgres.  
   Загрузка данных из Postgres в s3 в формате parquet и iceberg:  

   ```bash
   uv run main.py
   ```

Сравнение целевых данных в формате Parquet и Iceberg:  
Путь к bucket:  
```
data/minio/logs-bucket
```
Parquet формат содержит файлы с данными, и мета данные этих файлов.  
Iceberg формат добавляет к Parquet файлам(data) слой метаданных(metadata) для обеспечения ACID, версионирования.
