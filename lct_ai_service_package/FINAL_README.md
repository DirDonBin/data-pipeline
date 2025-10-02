# LCT AI Service Package - Готовый для интеграции!
## Что включено в пакет
### Основные компоненты
- **plugin_based_processor.py** - Система плагинов для разных источников данных
- **dag_centric_generator.py** - Генератор DAG с встроенными трансформациями
- **kafka_integration.py** - Интеграция с Kafka (ai-pipeline-request/response)
- **lct_format_adapter.py** - Адаптер форматов данных под ваши требования
- **llm_service.py** - Сервис интеграции с LLM (DeepSeek API)
### Docker Ready
- **Dockerfile.simple** - Оптимизированный Docker образ (~200MB)
- **docker-compose-lct-integration.yml** - Готовая конфигурация для Kafka
- **DOCKER_SUCCESS.md** - Инструкции по развертыванию
### Документация
- **README_FOR_TEAM.md** - Подробная техническая документация
- **INTEGRATION_CHECKLIST.md** - Чеклист интеграции
- **DOCKER_BUILD_INSTRUCTIONS.md** - Инструкции по сборке
## Архитектура под ваши требования
Система полностью переработана под ваш подход:
### Plugin-Based Architecture
```python
# Легко добавляются новые источники данных
processor.register_plugin("csv", CSVSourcePlugin())
processor.register_plugin("json", JSONSourcePlugin()) 
processor.register_plugin("postgres", PostgreSQLSourcePlugin())
```
### DAG-Centric Generation
```python
# Генерирует полные DAG с встроенными трансформациями
dag_generator = DAGCentricGenerator()
complete_dag = dag_generator.generate_complete_dag(schema_info)
# Результат: 7348 символов готового кода DAG
```
### Kafka Integration
```python
# Точно по вашему формату запросов
{
"request_id": "...",
"data_source": {...},
"processing_options": {...}
}
```
## Quick Start
1. **Сборка Docker образа:**
```bash
docker build -f Dockerfile.simple -t lct-ai-service:simple .
```
2. **Запуск сервиса:**
```bash
docker run -p 8080:8080 --env-file .env.lct lct-ai-service:simple
```
3. **Интеграция с Kafka:**
```bash
docker-compose -f docker-compose-lct-integration.yml up
```
## Тестирование
Локальные тесты показали:
- Plugin система работает корректно
- DAG генерация выдает 7348 символов готового кода
- Kafka интеграция соответствует вашему API
- Docker образ собирается и запускается
- Формат данных адаптирован под ваши требования
## Кастомизация
### Добавление новых плагинов
```python
class MySourcePlugin(SourcePlugin):
def get_schema(self, config): 
# Ваша логика
pass
```
### Настройка генерации DAG
```python
dag_config = {
"schedule_interval": "@daily",
"target_table_prefix": "processed_",
"transformation_rules": {...}
}
```
## Support
Все компоненты протестированы и готовы к работе. Система спроектирована под microservice архитектуру с четким разделением ответственности:
- **Test connection** Plugin система
- **Get schema** Schema инференция 
- **AI processing** LLM сервис
- **Return OK** Kafka response
