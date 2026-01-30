### Описание
Работа с Kafka. Стриминг данных в/из топика Kafka. Postgres -> Kafka -> Postgres.  

### Инструменты

- docker
- docker-compose
- uv
- PostgreSQL
- Apache Spark

### Запуск

1. Клонировать репозиторий и перейти в директорию проекта:

   ```bash
   git clone ...
   cd distributed_data_storage
   ```

2. Перейти на ветку kafka_streaming
   ```bash
   git checkout kafka_streaming
   ```

3. Создать .env файл с переменными окружения:

   ```bash
   cp .env.example .env
   ```

4. Запустить docker-compose.  
   ```bash
   make up
   ```
   или
   ```bash
   docker compose up -d
   ```

Сервис logs_sender генерирует и записывает логи в таблицу web_logs PostgreSQL с определенным интервалом.  
LOGS_SENDER_BATCH_SIZE - Сколько логов генерируется  
LOGS_SENDER_INTERVAL_SEC - Как часто

Сервис producer отправляет новые логи в топик KAFKA_TOPIC_NAME,
новые логи определяются по timestamp последнего забранного лога.  
Топик создается автоматически.  
PRODUCER_INTERVAL_SEC - Как часто проверять наличие новых логов

Сервис spark-consumer проверяет топик KAFKA_TOPIC_NAME на наличие логов
 и записывает их в в таблицу processed_data PostgreSQL с добавлением колонки processed_at

Проверка работы:
1) Топик Kafka:  
http://localhost:8080/ui/clusters/local-kafka/all-topics/logs_topic/messages?mode=TAILING&limit=100&r=r
где logs_topic это KAFKA_TOPIC_NAME

2) Заполнение таблиц web_logs и processed_data
   ```bash
   make watch_tables
   ```
   или
   ```bash
   watch -n 1 'docker compose exec postgres psql -U postgres -d mydb -c "SELECT '\''web_logs'\'', count(*) FROM web_logs UNION ALL SELECT '\''processed_data'\'', count(*) FROM processed_data;"'
   ```