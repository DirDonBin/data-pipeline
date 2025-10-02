# AI Data Engineer - Интеграция с командой ЛЦТ

## Краткое описание

**Intelligent Data Engineer** - AI сервис для автоматической генерации пайплайнов миграции данных с использованием DeepSeek API.

### Возможности:
- Анализ схем файлов (CSV/JSON/XML) и баз данных
- AI-рекомендации по выбору оптимального хранилища
- Генерация готовых Airflow DAG с встроенными трансформациями
- Автоматическая валидация и отчетность

## Архитектурная совместимость

### Соответствие вашему паттерну:
```
Test connection → Get schema → Request to AI → Return OK
```

### Плагинная система:
- **CSVFilePlugin** - обработка CSV файлов
- **JSONFilePlugin** - обработка JSON файлов  
- **PostgreSQLPlugin** - подключение к PostgreSQL
- Легко расширяется новыми плагинами

### DAG-центричная архитектура:
- Все DDL команды внутри DAG задач
- Встроенные трансформации данных
- Автоматическая валидация результатов
- Никаких отдельных миграционных скриптов

## Kafka интеграция

### Топики:
- **Входящий:** `ai-pipeline-request` (слушаем)
- **Исходящий:** `ai-pipeline-response` (отправляем)

### Формат данных:
- Полностью совместим с вашим JSON API
- Не требует изменений в data-processing сервисе

## Доставка результатов

### Множественные каналы:
1. **Kafka** - мгновенные уведомления
2. **Docker volumes** - файлы DAG, отчеты
3. **Airflow API** - прямая регистрация DAG
4. **PostgreSQL** - метаданные пайплайнов

### Что получаете:
- Готовый DAG файл в `/opt/airflow/dags/`
- Отчет по миграции в Markdown
- Метаданные в вашей БД

## Установка

### 1. Переменные окружения:
```bash
DEEPSEEK_API_KEY=ваш_ключ
KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092
```

### 2. Docker Compose:
```yaml
ai-intelligent-data-engineer:
  build: ./intelligent_data_engineer
  environment:
    - KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092
    - DEEPSEEK_API_KEY=${DEEPSEEK_API_KEY}
  depends_on:
    - kafka-broker
    - postgres
  volumes:
    - ./shared/ai_generated:/shared/ai_generated
    - ./airflow/dags:/opt/airflow/dags
```

### 3. Запуск:
```bash
docker-compose up -d ai-intelligent-data-engineer
```

## Безопасность

- **Исходные данные остаются локально**
- В DeepSeek передаются только метаданные (размеры, типы, статистика)
- Никаких персональных данных в облако

## Файлы для интеграции

### Основные:
- `plugin_based_processor.py` - плагинная система
- `dag_centric_generator.py` - генератор DAG
- `kafka_integration.py` - Kafka интеграция
- `lct_data_delivery.py` - доставка данных

### Docker:
- `Dockerfile` - образ сервиса
- `docker-compose-lct-integration.yml` - конфигурация
- `requirements_kafka.txt` - зависимости

### Документация:
- `LCT_INTEGRATION_GUIDE.md` - полная инструкция
- `DATA_DELIVERY_GUIDE.md` - каналы доставки
- `INTEGRATION_CHECKLIST.md` - чеклист интеграции

## Результат

После интеграции ваш пайплайн получит:
- AI-оптимизированный выбор хранилища
- Готовые DAG с встроенными трансформациями БД
- Автоматическую регистрацию в Airflow
- Подробные отчеты по миграции




