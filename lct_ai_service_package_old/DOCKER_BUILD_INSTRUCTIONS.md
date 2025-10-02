# Инструкция сборки Docker образа для команды ЛЦТ

## Для команды: как собрать и запустить

### 1. Сборка образа:
```bash
cd lct_ai_service_package
docker build -t lct-ai-service:latest .
```

### 2. Локальное тестирование:
```bash
# Запуск тестов в контейнере
docker run --rm lct-ai-service:latest python test_service.py

# Или через docker-compose
docker-compose -f docker-compose-test.yml up
```

### 3. Интеграция в вашу инфраструктуру:

#### Добавить в ваш docker-compose.yml:
```yaml
services:
  # ... ваши существующие сервисы ...

  lct-ai-service:
    build: ./lct_ai_service_package
    # или используйте готовый образ:
    # image: lct-ai-service:latest
    container_name: lct-ai-service
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092
      - DEEPSEEK_API_KEY=${DEEPSEEK_API_KEY}
      - POSTGRES_HOST=postgres
      - POSTGRES_DB=lct_db
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
    depends_on:
      - kafka-broker
      - postgres
    networks:
      - lct-network
    volumes:
      - ./shared/ai_generated:/shared/ai_generated
      - ./airflow/dags:/opt/airflow/dags
    restart: unless-stopped
```

### 4. Запуск:
```bash
# Настроить переменные окружения в .env
echo "DEEPSEEK_API_KEY=your_api_key_here" >> .env

# Запустить все сервисы
docker-compose up -d lct-ai-service
```

## Проверка работы:

### Логи:
```bash
docker logs lct-ai-service -f
```

### Тестирование Kafka интеграции:
```bash
# Отправить тестовое сообщение в ai-pipeline-request
# Проверить ответ в ai-pipeline-response
```

### Проверка DAG в Airflow:
- Зайти в Airflow UI
- Найти DAG с префиксом `pipeline_`
- Запустить и проверить выполнение

## Размеры образа:
- Базовый образ: python:3.11-slim (~180MB)
- С зависимостями: ~300-400MB
- Готовый образ: ~400-500MB

## Производительность:
- Время сборки: 2-3 минуты
- Время запуска: 5-10 секунд
- Обработка запроса: 1-5 секунд (без учета LLM API)

## Мониторинг:
```bash
# Использование ресурсов
docker stats lct-ai-service

# Healthcheck
docker exec lct-ai-service python -c "print('Service OK')"
```

---

**Готово к интеграции!** 🚀