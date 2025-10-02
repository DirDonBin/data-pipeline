# Чеклист передачи AI сервиса команде ЛЦТ

## Файлы для интеграции

### Основные файлы сервиса:
- [x] `plugin_based_processor.py` - плагинная система для источников данных
- [x] `dag_centric_generator.py` - генератор DAG с встроенными трансформациями
- [x] `kafka_integration.py` - интеграция с их Kafka инфраструктурой
- [x] `lct_format_adapter.py` - адаптер форматов данных
- [x] `lct_data_delivery.py` - система доставки сгенерированных данных

### Docker конфигурация:
- [x] `Dockerfile` - образ AI сервиса
- [x] `docker-compose-lct-integration.yml` - интеграция в их docker-compose
- [x] `requirements_kafka.txt` - зависимости для Kafka
- [x] `.env.lct` - переменные окружения

### Документация:
- [x] `LCT_INTEGRATION_GUIDE.md` - главная инструкция интеграции
- [x] `LCT_ARCHITECTURE_ALIGNMENT.md` - соответствие архитектурам
- [x] `DATA_DELIVERY_GUIDE.md` - как доставляются данные
- [x] `REAL_LLM_PROMPTS_EXAMPLE.md` - примеры работы с AI

## Что получает команда ЛЦТ

### 1. Готовый к запуску AI сервис:
```
docker-compose up -d ai-intelligent-data-engineer
```

### 2. Плагинная архитектура:
- CSVFilePlugin - обработка CSV файлов
- JSONFilePlugin - обработка JSON файлов  
- PostgreSQLPlugin - подключение к PostgreSQL
- Легко расширяется новыми плагинами

### 3. DAG-центричные пайплайны:
- Создание схемы БД внутри DAG
- Извлечение данных из источников
- AI-рекомендованные трансформации
- Загрузка в целевое хранилище
- Автоматическая валидация

### 4. Kafka интеграция:
- Слушает: `ai-pipeline-request`
- Отправляет: `ai-pipeline-response`
- Формат совместим с их JSON API

### 5. Множественные каналы доставки:
- Kafka сообщения (мгновенно)
- Общие Docker volumes (файлы)
- Прямая регистрация в Airflow
- Сохранение в PostgreSQL

## Настройка для команды

### Шаг 1: Переменные окружения
```bash
# Обязательно настроить:
DEEPSEEK_API_KEY=ваш_api_ключ
KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092

# Остальное по умолчанию подходит
```

### Шаг 2: Docker volumes
```yaml
volumes:
  - ./shared/ai_generated:/shared/ai_generated
  - ./airflow/dags:/opt/airflow/dags
```

### Шаг 3: Добавить сервис в docker-compose.yml
```yaml
ai-intelligent-data-engineer:
  build: ./intelligent_data_engineer
  environment:
    - KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092
    - DEEPSEEK_API_KEY=${DEEPSEEK_API_KEY}
  depends_on:
    - kafka-broker
    - postgres
```

## Архитектурное соответствие

### Их требования ✓
- [x] Test connection → плагинная система
- [x] Get schema → специфично для каждого плагина  
- [x] Request to AI → LLM анализ через DeepSeek
- [x] Return OK → DAG + отчет

### Их паттерн ✓
- [x] Микросервис максимально обобщен
- [x] Плагины для конкретных типов источников
- [x] DAG файл содержит всю миграцию
- [x] Никаких отдельных DDL скриптов

### Их workflow ✓
```
Response from AI → save dag file → save pipeline in db → client notification
```

## Что НЕ нужно команде

### Старые файлы (можно удалить):
- INTEGRATION_API.md - заменен на LCT_INTEGRATION_GUIDE.md
- DOCKER_INTEGRATION.md - объединено в главный гид
- LCT_INTEGRATION_EXAMPLE.md - заменено реальными примерами

### Отдельные модули (не нужны с новой архитектурой):
- src/modules/* - заменены плагинной системой
- Отдельные DDL генераторы - теперь внутри DAG
- Миграционные скрипты - все в DAG

## Проверочный список

### Технические требования:
- [x] Kafka Consumer/Producer настроен
- [x] JSON формат запросов/ответов совместим
- [x] Docker контейнер готов к запуску
- [x] Все зависимости в requirements

### Функциональные требования:
- [x] Test connection для разных источников
- [x] Get schema через плагины
- [x] AI анализ через DeepSeek
- [x] DAG генерация с встроенными трансформациями
- [x] Множественная доставка результатов

### Интеграционные требования:
- [x] Совместимость с их Kafka топиками
- [x] Подключение к их PostgreSQL
- [x] Регистрация DAG в их Airflow
- [x] Использование их Docker сети

## Итоговый пакет для команды

### Архив должен содержать:
```
intelligent_data_engineer/
├── plugin_based_processor.py
├── dag_centric_generator.py  
├── kafka_integration.py
├── lct_format_adapter.py
├── lct_data_delivery.py
├── Dockerfile
├── docker-compose-lct-integration.yml
├── requirements_kafka.txt
├── .env.lct
├── LCT_INTEGRATION_GUIDE.md
├── LCT_ARCHITECTURE_ALIGNMENT.md
├── DATA_DELIVERY_GUIDE.md
└── REAL_LLM_PROMPTS_EXAMPLE.md
```

### Инструкция запуска (одна команда):
```bash
# 1. Настроить .env файл с DEEPSEEK_API_KEY
# 2. Запустить:
docker-compose -f docker-compose-lct-integration.yml up -d
```
