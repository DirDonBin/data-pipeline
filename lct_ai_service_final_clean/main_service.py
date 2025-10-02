"""
Main Service - FastAPI приложение с HTTP и Kafka интеграцией
Исправленная версия с поддержкой всех требований fix.md
"""
import json
import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

from config_loader import load_config, get_config
from updated_lct_service import UpdatedLCTService

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация FastAPI
app = FastAPI(
    title="LCT AI Service",
    description="AI сервис для генерации пайплайнов миграции данных",
    version="2.0"
)

# CORS настройки
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Инициализация сервисов
config_obj = load_config()  # Объект конфигурации
lct_service = UpdatedLCTService()
kafka_integration = None

# Инициализация Kafka (опционально)
kafka_enabled = get_config("kafka.enabled", False)
if kafka_enabled:
    try:
        from kafka_integration import KafkaIntegration, start_kafka_consumer_thread
        kafka_integration = start_kafka_consumer_thread()
        logger.info("Kafka интеграция активирована")
    except ImportError:
        logger.warning("Kafka библиотека не найдена, работаем только через HTTP")
    except Exception as e:
        logger.error(f"Ошибка инициализации Kafka: {e}")

class PipelineRequest(BaseModel):
    """Модель запроса для генерации пайплайна в новом формате fix.md"""
    request_uid: str
    pipeline_uid: str
    timestamp: Optional[str] = ""
    sources: list
    targets: list
    metadata: Optional[dict] = {}

class NodeConfig(BaseModel):
    """Конфигурация ноды"""
    level: int
    spark_distributed: bool
    from_node: str = Field(..., alias="from")
    to_node: str = Field(..., alias="to")
    
    class Config:
        allow_population_by_field_name = True

class Node(BaseModel):
    """Модель ноды пайплайна с поддержкой Spark"""
    id: str
    type: str
    name: str
    name_ru: str
    description: str
    config: NodeConfig
    code: str  # Основной код функции
    # Дополнительные поля для Spark/DAG
    task_name: str  # Название задачи в DAG
    dag_task_definition: str  # Определение задачи для DAG
    function_filename: str  # Имя файла функции  
    function_code: str  # Код функции для отдельного файла
    # Поля для целевых хранилищ
    target_storage: Optional[str] = None  # Рекомендуемое хранилище (ClickHouse/PostgreSQL/HDFS)
    target_reasoning: Optional[str] = None  # Обоснование выбора хранилища
    storage_characteristics: Optional[dict] = None  # Характеристики данных для хранилища

class Connection(BaseModel):
    """Модель связи между нодами"""
    from_node: str = Field(..., alias="from")
    to_node: str = Field(..., alias="to")
    
    class Config:
        allow_population_by_field_name = True

class PipelineResponse(BaseModel):
    """Модель ответа с пайплайном в новом формате fix.md"""
    request_uid: str
    pipeline_uid: str
    status: str
    created_at: str
    template: str
    pipelines: list  # Старый формат для совместимости с fix.md
    nodes: List[Node]  # Новый удобный формат
    connections: List[Connection]  # Связи между нодами
    dag_assembly: Optional[dict] = {}  # Блоки для сборки DAG
    spark_distributed: Optional[bool] = False  # Флаг использования Spark
    migration_report: str
    metadata: Optional[dict] = {}

@app.get("/")
async def root():
    """Главная страница API"""
    return {
        "service": "LCT AI Service",
        "version": "2.0",
        "description": "AI сервис для генерации пайплайнов миграции данных",
        "endpoints": {
            "generate_pipeline": "/generate-pipeline",
            "health": "/health",
            "status": "/status"
        },
        "features": [
            "HTTP API",
            "Kafka интеграция" if kafka_integration else "HTTP только",
            "DeepSeek LLM",
            "Hadoop пайплайны",
            "Migration отчеты"
        ]
    }

@app.post("/generate-pipeline", response_model=PipelineResponse)
async def generate_pipeline(request: PipelineRequest):
    """Генерация пайплайна миграции данных"""
    try:
        logger.info(f"Получен запрос на генерацию пайплайна: {request.request_uid}")
        
        # Конвертация в словарь для обработки
        request_dict = request.dict()
        
        # Обработка через LCT Service
        result = await lct_service.process_new_format_request(request_dict)
        
        # Проверка результата
        if result.get("status") == "error":
            raise HTTPException(
                status_code=500, 
                detail=result.get("error", "Ошибка генерации пайплайна")
            )
        
        # Извлекаем nodes и connections из generated_dag для совместимости с PipelineResponse
        generated_dag = result.get("generated_dag", {})
        result["nodes"] = generated_dag.get("nodes", [])
        result["connections"] = generated_dag.get("connections", [])
        
        logger.info(f"Пайплайн успешно сгенерирован: {result['request_uid']}")
        logger.info(f"Migration report: {len(result.get('migration_report', ''))} символов")
        logger.info(f"Nodes: {len(result['nodes'])}, Connections: {len(result['connections'])}")
        
        return PipelineResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка генерации пайплайна: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Проверка состояния сервиса"""
    try:
        # Проверка основных компонентов
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "version": "2.0",
            "components": {
                "lct_service": "ok",
                "config": "ok",
                "llm_service": "ok"
            }
        }
        
        # Проверка Kafka если включен
        if kafka_integration:
            try:
                kafka_status = kafka_integration.get_kafka_status()
                health_status["components"]["kafka"] = "ok" if kafka_status.get("connected") else "error"
            except:
                health_status["components"]["kafka"] = "error"
        
        return health_status
        
    except Exception as e:
        logger.error(f"Ошибка health check: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/status")
async def get_status():
    """Детальный статус сервиса"""
    try:
        # Базовый статус
        status = {
            "service": "LCT AI Service",
            "version": "2.0",
            "uptime": datetime.now().isoformat(),
            "configuration": {
                "kafka_enabled": get_config("kafka.enabled", False),
                "llm_provider": get_config("deepseek.provider", "deepseek"),
                "max_tokens": get_config("deepseek.max_tokens")
            }
        }
        
        # Статус LCT Service
        try:
            lct_status = lct_service.get_service_status()
            status["lct_service"] = lct_status
        except Exception as e:
            status["lct_service"] = {"status": "error", "error": str(e)}
        
        # Статус Kafka
        if kafka_integration:
            try:
                kafka_status = kafka_integration.get_kafka_status()
                status["kafka"] = kafka_status
            except Exception as e:
                status["kafka"] = {"status": "error", "error": str(e)}
        else:
            status["kafka"] = {"status": "disabled"}
        
        return status
        
    except Exception as e:
        logger.error(f"Ошибка получения статуса: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test-kafka")
async def test_kafka_connection():
    """Тестирование подключения к Kafka"""
    if not kafka_integration:
        raise HTTPException(
            status_code=503, 
            detail="Kafka интеграция не активирована"
        )
    
    try:
        test_result = kafka_integration.test_connection()
        return test_result
        
    except Exception as e:
        logger.error(f"Ошибка тестирования Kafka: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/kafka/send-request")
async def send_kafka_request(request: PipelineRequest):
    """Отправка запроса через Kafka"""
    if not kafka_integration:
        raise HTTPException(
            status_code=503, 
            detail="Kafka интеграция не активирована"
        )
    
    try:
        request_dict = request.dict()
        success = kafka_integration.send_pipeline_request(
            request_dict, 
            key=request.uid
        )
        
        if not success:
            raise HTTPException(
                status_code=500, 
                detail="Не удалось отправить запрос в Kafka"
            )
        
        return {
            "status": "sent",
            "message": "Запрос отправлен в Kafka",
            "uid": request.uid
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка отправки в Kafka: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Обработчик завершения работы
@app.on_event("shutdown")
async def shutdown_event():
    """Обработчик завершения работы приложения"""
    logger.info("Завершение работы LCT AI Service...")
    
    if kafka_integration:
        try:
            kafka_integration.stop()
            logger.info("Kafka интеграция остановлена")
        except Exception as e:
            logger.error(f"Ошибка остановки Kafka: {e}")

@app.post("/analyze_migration")
async def analyze_migration(request: PipelineRequest):
    """Анализ данных и рекомендации по миграции"""
    try:
        logger.info("Получен запрос на анализ миграции данных")
        
        # Извлекаем источники данных из запроса
        sources = []
        if hasattr(request, 'sources') and request.sources:
            sources = [source.dict() for source in request.sources]
        elif hasattr(request, 'tables') and request.tables:
            # Преобразуем таблицы в источники
            for table in request.tables:
                sources.append({
                    "name": table.name,
                    "table_type": table.table_type,
                    "columns": [col.dict() for col in table.columns] if table.columns else [],
                    "estimated_rows": getattr(table, 'estimated_rows', 100000)
                })
        
        if not sources:
            raise HTTPException(status_code=400, detail="Не найдены источники данных для анализа")
        
        # Анализ миграции
        result = await lct_service.analyze_data_migration(sources)
        
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("error"))
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка анализа миграции: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":

    # Запуск сервера
    port = get_config("server.port", 8080)
    host = get_config("server.host", "0.0.0.0")
    
    logger.info(f"Запуск LCT AI Service на {host}:{port}")
    
    uvicorn.run(
        "main_service:app",
        host=host,
        port=port,
        log_level="info",
        reload=False
    )
