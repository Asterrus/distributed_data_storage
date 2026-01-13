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
```
├── iceberg
│   └── default
│       └── web_logs
│           ├── data
│           │   └── 00000-0-d04c2138-1889-4caf-9a2c-8af70c66fef5.parquet
│           │       ├── d065b3ac-cf76-4649-a0c7-aba3eebff4e0
│           │       │   └── part.1
│           │       └── xl.meta
│           └── metadata
│               ├── 00000-6cd835c3-6887-48d6-b218-1f51d078c562.metadata.json
│               │   ├── df5a5883-675c-421c-a333-e882b7b08e95
│               │   │   └── part.1
│               │   └── xl.meta
│               ├── 00001-71012813-e0b8-4882-9195-1a244cc95424.metadata.json
│               │   ├── b8d7eb8f-cbb9-44f2-93cb-19aa426c9bd3
│               │   │   └── part.1
│               │   └── xl.meta
│               ├── d04c2138-1889-4caf-9a2c-8af70c66fef5-m0.avro
│               │   ├── bfafaefb-5611-45b7-b671-2cc8db87c850
│               │   │   └── part.1
│               │   └── xl.meta
│               └── snap-1567958721510262377-0-d04c2138-1889-4caf-9a2c-8af70c66fef5.avro
│                   ├── d90ebc1f-21f9-4d3f-bb96-5b1ef37aa0b6
│                   │   └── part.1
│                   └── xl.meta
└── parquet
    └── web_logs
        └── web_logs.parquet
            ├── 478fb5d1-9bdf-43e7-9a40-b910d40556d7
            │   └── part.1
            └── xl.meta
```


Parquet формат содержит файлы с данными(part.*), и мета данные этих файлов(xl.meta).  

Iceberg формат добавляет к Parquet файлам(data) слой метаданных(metadata) для обеспечения ACID, версионирования, хранения схем таблиц, безопасной конкурентной записи.

Слой metadata Iceberg:
1. UUID.metadata.json файлы - Метаинформация таблицы, для разных версий таблицы
2. UUID.avro - манифест файлы, реестр всех файлов Parquet
3. snap-UUID.avro - Snapshot файлы, снимки состояний таблицы в разное время
