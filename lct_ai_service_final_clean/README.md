# LCT AI Service - Система генерации пайплайнов миграции данных

## Описание

LCT AI Service представляет собой интеллектуальную систему для автоматической генерации пайплайнов миграции данных с поддержкой Apache Spark и анализом оптимальных целевых хранилищ. Система использует LLM для анализа характеристик данных и предоставляет детальные рекомендации по выбору архитектуры хранения.

## Ключевые возможности

### Автоматическая генерация пайплайнов
- Создание Apache Airflow DAG файлов на основе спецификации источников и целей
- Интеллектуальное определение необходимости использования Apache Spark
- Поддержка как распределенной (Spark), так и локальной обработки данных

### Анализ и рекомендации хранилищ
- Автоматический анализ характеристик данных (объем, скорость, разнообразие, сложность)
- Рекомендации оптимальных целевых систем: ClickHouse, PostgreSQL, HDFS
- Детальное обоснование выбора с техническими причинами

### Архитектурная гибкость
- Модульная структура с возможностью настройки компонентов
- Поддержка различных форматов данных (JSON, CSV, Parquet, XML)
- Интеграция с Kafka для потоковой обработки

## Архитектура системы

### Основные компоненты

```
├── main_service.py              # FastAPI веб-сервис
├── updated_lct_service.py       # Основная бизнес-логика
├── hadoop_dag_generator.py      # Генератор Airflow DAG
├── data_migration_analyzer.py   # Анализатор данных и рекомендаций
├── lct_format_adapter.py        # Адаптер форматов данных
├── llm_service.py              # Интеграция с языковыми моделями
└── kafka_integration.py        # Интеграция с Apache Kafka
```

### Технологический стек
- **Backend**: Python 3.11+, FastAPI
- **Обработка данных**: Apache Spark, Apache Airflow
- **Базы данных**: PostgreSQL, ClickHouse, HDFS
- **Потоковая обработка**: Apache Kafka
- **ИИ/ML**: DeepSeek API для генерации рекомендаций

## Установка и настройка

### Требования к системе
- Python 3.11 или выше
- Apache Spark 3.x (для распределенной обработки)
- Apache Airflow 2.x (для оркестрации)
- PostgreSQL или ClickHouse (для метаданных)

### Установка зависимостей
```bash
pip install -r requirements.txt
```

### Конфигурация
Создайте файл `config.json`:

```json
{
  "llm": {
    "api_key": "your_deepseek_api_key",
    "base_url": "https://api.deepseek.com",
    "model": "deepseek-chat",
    "timeout": 30
  },
  "kafka": {
    "enabled": false,
    "bootstrap_servers": "localhost:9092",
    "topic": "data_migration_requests"
  },
  "spark": {
    "app_name": "LCT_DataMigration",
    "master": "local[*]",
    "config": {
      "spark.sql.adaptive.enabled": "true",
      "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }
  }
}
```

### Запуск сервиса
```bash
python main_service.py
```

Сервис будет доступен по адресу: `http://localhost:8000`

## API Спецификация

### Генерация пайплайна миграции

**Endpoint**: `POST /generate-pipeline`

**Формат запроса**:
```json
{
  "request_uid": "unique-request-id",
  "pipeline_uid": "unique-pipeline-id",
  "sources": [
    {
      "name": "source_name",
      "format": "json|csv|postgresql|parquet",
      "estimated_size": "1GB|100TB",
      "source_type": "database|streaming|files",
      "update_frequency": "realtime|high|daily|batch",
      "data_characteristics": {
        "volume": "low|medium|high|very_high",
        "velocity": "batch|high|realtime",
        "variety": "structured|semi_structured|unstructured",
        "complexity": "simple|medium|high"
      }
    }
  ],
  "targets": [
    {
      "name": "target_name",
      "format": "postgresql|columnar|parquet",
      "requirements": ["analytics", "real_time_processing", "backup_support"]
    }
  ]
}
```

**Формат ответа**:
```json
{
  "request_uid": "unique-request-id",
  "pipeline_uid": "unique-pipeline-id",
  "status": "success",
  "template": "airflow_dag_template_code",
  "generated_dag": {
    "nodes": [
      {
        "task_name": "spark_extract_task",
        "dag_task_definition": "SparkSubmitOperator_definition",
        "function_filename": "spark_extract.py",
        "function_code": "pyspark_function_code",
        "target_storage": "ClickHouse|PostgreSQL|HDFS",
        "target_reasoning": "technical_justification"
      }
    ],
    "connections": [
      {"from": "task_1", "to": "task_2"}
    ]
  },
  "migration_recommendations": [
    {
      "source_name": "source_name",
      "primary": "ClickHouse",
      "secondary": "HDFS", 
      "reasoning": "detailed_technical_explanation"
    }
  ],
  "spark_distributed": true,
  "migration_report": "detailed_text_report"
}
```

## Логика принятия решений

### Определение использования Spark

Система автоматически активирует Apache Spark при наличии следующих условий:
- Объем данных превышает 1GB
- Тип источника: streaming
- Частота обновления: realtime или high
- Характеристики данных: volume=very_high, velocity=realtime, complexity=high
- Количество источников больше 2
- Общий объем всех источников превышает 2GB

### Рекомендации целевых хранилищ

**ClickHouse** рекомендуется для:
- Аналитических workload с временными данными
- Больших объемов данных (>50MB) с числовыми метриками
- OLAP операций и агрегаций

**PostgreSQL** рекомендуется для:
- Транзакционных данных среднего объема
- Структурированных данных, требующих ACID свойств
- OLTP операций

**HDFS** рекомендуется для:
- Очень больших объемов данных (>1GB)
- Долгосрочного хранения архивных данных
- Сырых данных для последующей обработки

## Интеграция с командой

### Формат ответа для фронтенда
Система возвращает данные в формате, совместимом с требованиями команды:

```json
{
  "request_uid": "uuid-запроса",
  "pipeline_uid": "uuid-пайплайна", 
  "status": "success",
  "created_at": "2025-10-02T15:57:02.989780",
  "spark_distributed": true,
  "template": "полный код DAG",
  "generated_dag": {
    "nodes": [
      {
        "task_name": "spark_extract_task",
        "dag_task_definition": "определение задачи",
        "function_filename": "имя файла функции",
        "function_code": "код функции"
      }
    ],
    "connections": [
      {"from": "node1", "to": "node2"}
    ]
  },
  "migration_report": "детальный текстовый отчет"
}
```

### Извлечение данных для десериализации
```python
# Извлечение узлов для отдельного сохранения
for node in response['generated_dag']['nodes']:
    task_name = node['task_name']
    dag_definition = node['dag_task_definition'] 
    filename = node['function_filename']
    code = node['function_code']
    
    # Сохранение каждого компонента отдельно
    save_component(task_name, dag_definition, filename, code)

# Извлечение связей
for connection in response['generated_dag']['connections']:
    from_node = connection['from']
    to_node = connection['to']
    save_connection(from_node, to_node)
```

## Интеграция с фронтендом

### Пример использования в JavaScript
```javascript
// Получение ответа от API
const response = await fetch('/generate-pipeline', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify(requestData)
});
const data = await response.json();

// Извлечение компонентов для сборки DAG
data.generated_dag.nodes.forEach(node => {
  console.log(`Task: ${node.task_name}`);
  console.log(`File: ${node.function_filename}`);  
  console.log(`DAG Definition: ${node.dag_task_definition}`);
  console.log(`Function Code: ${node.function_code}`);
});

// Построение зависимостей
data.generated_dag.connections.forEach(conn => {
  console.log(`${conn.from} >> ${conn.to}`);
});

// Сборка финального DAG
let finalDAG = data.template;
finalDAG = finalDAG.replace('{{FUNCTIONS}}', collectAllFunctions(data.generated_dag.nodes));
finalDAG = finalDAG.replace('{{TASK_SEQUENCE}}', buildTaskSequence(data.generated_dag.nodes));
finalDAG = finalDAG.replace('{{DEPENDENCIES}}', buildDependencies(data.generated_dag.connections));
```

## Мониторинг и диагностика

### Логирование
Система использует стандартную библиотеку logging Python с следующими уровнями:
- `INFO`: Общая информация о процессе генерации
- `WARNING`: Предупреждения о потенциальных проблемах
- `ERROR`: Ошибки выполнения операций

### Метрики производительности
- Время генерации пайплайна
- Размер обрабатываемых данных
- Количество сгенерированных рекомендаций
- Статистика использования Spark vs локальной обработки

## Тестирование

### Запуск тестов
```bash
# Тест соответствия требованиям команды
python test_team_compliance.py

# Тест автоматического определения Spark
python test_spark_detection.py

# Тест совместимости с фронтендом
python test_frontend_compatibility.py

# Тест генерации миграционных рекомендаций
python test_migration_text.py
```

### Примеры тестовых сценариев
- Обработка больших объемов данных (>1TB)
- Потоковые данные в реальном времени
- Миграция между различными типами хранилищ
- Валидация структуры API ответов

## Развертывание в продакшене

### Docker контейнеризация
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["python", "main_service.py"]
```

### Конфигурация для продакшена
- Использование внешних сервисов для PostgreSQL и ClickHouse
- Настройка Apache Kafka для высокой доступности
- Конфигурация Spark кластера для распределенной обработки
- Настройка мониторинга и алертинга

## Лицензия

Данный проект разработан для хакатона ЛЦТ 2025 и предназначен для демонстрации возможностей автоматической генерации пайплайнов миграции данных.

## Контакты и поддержка

Для вопросов по использованию и развитию системы обращайтесь к команде разработки проекта ЛЦТ AI Service.

## Дополнительная информация

### Основные библиотеки
- FastAPI - веб-фреймворк
- pydantic - валидация данных
- kafka-python - интеграция с Kafka
- requests - HTTP клиент для LLM
- uvicorn - ASGI сервер

### Архитектура системы

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   HTTP Client   │    │  Kafka Producer │    │  LLM Service    │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FastAPI Application                         │
├─────────────────────────────────────────────────────────────────┤
│                  UpdatedLCTService                             │
├─────────────────────────────────────────────────────────────────┤
│  FormatAdapter  │  HadoopDAGGenerator  │     LLMService        │
│                 │  DataMigrationAnalyzer │                     │
└─────────────────────────────────────────────────────────────────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Validation    │    │  Spark Detection│    │  DeepSeek API   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### API для анализа миграции

**POST /analyze_migration**

Анализирует данные и предоставляет рекомендации по миграции в ClickHouse, PostgreSQL или HDFS.

**Пример запроса:**
```bash
curl -X POST "http://localhost:8080/analyze_migration" \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [
      {
        "name": "user_transactions",
        "table_type": "transactional",
        "estimated_rows": 5000000,
        "file_size_mb": 150,
        "format": "parquet",
        "columns": [
          {"name": "user_id", "type": "int"},
          {"name": "transaction_date", "type": "datetime"},
          {"name": "amount", "type": "decimal"}
        ]
      }
    ]
  }'
```

**Формат ответа:**
```json
{
  "status": "success",
  "migration_analysis": {
    "sources_analyzed": 1,
    "individual_recommendations": [
      {
        "source": "user_transactions",
        "characteristics": {
          "volume": "medium",
          "complexity": "medium",
          "performance_needs": "high"
        },
        "recommendations": {
          "primary": "ClickHouse",
          "secondary": "PostgreSQL",
          "reasoning": "Высокая производительность аналитических запросов"
        }
      }
    ],
    "overall_strategy": {
      "recommended_approach": "hybrid",
      "migration_steps": ["analysis", "setup", "migration", "validation"],
      "estimated_time": "2-3 weeks"
    }
  }
}
```

## Поддержка

Для вопросов и поддержки обращайтесь к команде разработки.