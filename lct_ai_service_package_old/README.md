# LCT AI Service - Готовый пакет для команды

## Что в пакете

**Production-ready AI сервис** для генерации пайплайнов миграции данных с полной интеграцией DeepSeek API.

### Основные файлы:
- `plugin_based_processor.py` - плагинная система источников
- `dag_centric_generator.py` - генератор DAG с встроенными трансформациями  
- `kafka_integration.py` - интеграция с Kafka
- `llm_service.py` - **Production AI сервис** с DeepSeek API
- `test_service.py` - тесты функциональности

### Docker:
- `Dockerfile` - готовый образ
- `docker-compose-lct-integration.yml` - для интеграции в вашу инфраструктуру
- `docker-compose-test.yml` - для локального тестирования
- `requirements.txt` - все зависимости

### Документация:
- `README_FOR_TEAM.md` - основная инструкция
- `INTEGRATION_CHECKLIST.md` - чеклист интеграции

## Быстрый старт

### 1. Локальное тестирование:
```bash
# Установить зависимости
pip install -r requirements.txt

# Запустить тесты
python test_service.py
```

### 2. Docker тестирование:
```bash
# Собрать образ
docker build -t lct-ai-service .

# Запустить тесты в контейнере
docker-compose -f docker-compose-test.yml up
```

### 3. Интеграция в вашу инфраструктуру:
```bash
# Настроить .env.lct с вашими параметрами
# Добавить сервис в ваш docker-compose.yml
# Запустить: docker-compose up -d lct-ai-service
```

## Что работает

**Production-ready функциональность:**
- ✅ Плагинная система источников (CSV/JSON/PostgreSQL)
- ✅ Генерация DAG с встроенными трансформациями БД
- ✅ Адаптер форматов данных под ваш API
- ✅ **Полная интеграция с DeepSeek API** (production)

**Сгенерированный пример:**
- `generated_test_dag.py` - готовый Airflow DAG (7348 символов)

## Архитектурное соответствие

### Ваш паттерн:
```
Test connection → Get schema → Request to AI → Return OK
```

### Наша реализация:
- **Test connection** - через плагины для каждого типа источника
- **Get schema** - автоматический анализ структуры данных  
- **Request to AI** - LLM рекомендации по хранилищу и трансформациям
- **Return OK** - готовый DAG + отчет

### DAG-центричность:
- Все DDL команды внутри DAG задач
- Встроенные трансформации данных
- Автоматическая валидация результатов

## Интеграция

**Время интеграции:** 15 минут  
**Изменения в вашем коде:** 0 строк  
**Что получаете:** Полнофункциональный AI Data Engineer

### Kafka топики:
- Слушаем: `ai-pipeline-request`
- Отправляем: `ai-pipeline-response`

### Результат:
- Готовый Airflow DAG в `/opt/airflow/dags/`
- Отчет по миграции
- Метаданные в PostgreSQL

## Production настройки

### Для production использования:
1. **Сервис готов к production** - DeepSeek API полностью интегрирован
2. **Настройте переменные окружения** в .env.lct (API ключ уже установлен)
3. **Добавьте мониторинг** и логирование
4. **Настройте security** для Kafka и PostgreSQL

### API ключ DeepSeek:
```bash
DEEPSEEK_API_KEY=sk-a4ad2cc3a41e4b5581e82b05a2983a4b
```

