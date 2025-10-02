"""
LCT AI Service - Единый главный сервис
Объединяет все компоненты: новый формат, Hadoop, Spark, callback поддержку
Все требования fix.md реализованы в одном файле
"""

import json
import logging
import asyncio
import httpx
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

# Импорт наших модулей
from updated_lct_service import UpdatedLCTAIService
from config_loader import config, get_config, get_api_config

# Настройка логирования из конфигурации
logging.basicConfig(
    level=getattr(logging, config.log_level.upper()),
    format=get_config('logging.format', '[%(levelname)s] %(name)s: %(message)s')
)
logger = logging.getLogger(__name__)

# Попытка импорта Kafka интеграции (может быть недоступен если kafka-python не установлен)
try:
    from kafka_integration import start_kafka_integration_background
    KAFKA_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Kafka интеграция недоступна: {e}")
    KAFKA_AVAILABLE = False

# Глобальные переменные для сервисов
lct_service_standard = None
lct_service_spark = None
kafka_integration = None
kafka_task = None


# ========== PYDANTIC MODELS ==========

class PipelineRequest(BaseModel):
    """Модель запроса в новом формате (fix.md)"""
    request_uid: str
    pipeline_uid: str
    timestamp: str
    sources: List[Dict[str, Any]]  # Новый формат с $type, parameters, schema_infos
    targets: List[Dict[str, Any]]  # Новый формат с $type, parameters
    spark_enabled: Optional[bool] = False
    callback_url: Optional[str] = None


class HealthResponse(BaseModel):
    """Модель для health check"""
    status: str
    timestamp: str
    version: str
    components: Dict[str, str]


# ========== CALLBACK FUNCTIONS ==========

async def send_callback_notification(callback_url: str, result_data: Dict[str, Any]):
    """Отправка уведомления на callback URL после завершения обработки"""
    if not callback_url:
        return
    
    try:
        logger.info(f"[CALLBACK] Отправка уведомления на {callback_url}")
        
        # Данные для callback
        callback_data = {
            "request_uid": result_data.get("request_uid"),
            "status": result_data.get("status"),
            "processing_time": result_data.get("processing_time", 0),
            "pipelines_count": len(result_data.get("pipelines", [])),
            "completed_at": datetime.now().isoformat(),
            "spark_enabled": result_data.get("spark_distribution_enabled", False)
        }
        
        # Используем настройки из конфигурации
        timeout = get_config('callbacks.timeout', 30)
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                callback_url,
                json=callback_data,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                logger.info(f"[CALLBACK] Уведомление успешно отправлено")
            else:
                logger.warning(f"[CALLBACK] Ошибка callback: {response.status_code}")
                
    except httpx.ConnectError as e:
        logger.warning(f"[CALLBACK] Не удается подключиться к {callback_url}: {e}")
    except httpx.TimeoutException as e:
        logger.warning(f"[CALLBACK] Таймаут при отправке callback на {callback_url}: {e}")
    except Exception as e:
        logger.error(f"[CALLBACK] Ошибка отправки callback: {e}")


# ========== GLOBAL SERVICES ==========
lct_service_standard: Optional[UpdatedLCTAIService] = None
lct_service_spark: Optional[UpdatedLCTAIService] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    global lct_service_standard, lct_service_spark, kafka_integration, kafka_task
    
    # Startup
    logger.info("[LAUNCH] Запуск LCT AI Service...")
    
    try:
        # Инициализация двух версий сервиса
        lct_service_standard = UpdatedLCTAIService(enable_spark_distribution=False)
        lct_service_spark = UpdatedLCTAIService(enable_spark_distribution=True)
        
        # Инициализация Kafka интеграции если доступна
        if KAFKA_AVAILABLE:
            try:
                kafka_config = get_config('kafka', {})
                if kafka_config.get('enabled', False):
                    kafka_integration, kafka_task = await start_kafka_integration_background(kafka_config)
                    logger.info("[KAFKA] Kafka интеграция запущена")
                else:
                    logger.info("[KAFKA] Kafka интеграция отключена в конфигурации")
            except Exception as e:
                logger.error(f"[KAFKA] Ошибка запуска Kafka интеграции: {e}")
                kafka_integration = None
                kafka_task = None
        else:
            logger.info("[KAFKA] Kafka модуль недоступен")
        
        logger.info("[SUCCESS] Все сервисы готовы к работе")
        logger.info("[FEATURES] Поддерживаемые функции:")
        logger.info("   - Новый формат запросов ($type, parameters, schema_infos)")
        logger.info("   - Hadoop пути (автоконвертация в HDFS)")
        logger.info("   - Template + pipelines ответ")
        logger.info("   - Опциональные Spark функции")
        logger.info("   - Callback уведомления")
        if kafka_integration:
            logger.info("   - Kafka интеграция (слушаем ai-pipeline-request)")
        
        yield
        
    except Exception as e:
        logger.error(f"[ERROR] Ошибка инициализации: {e}")
        raise
    
    # Shutdown
    logger.info("[SHUTDOWN] Остановка LCT AI Service...")
    
    # Остановка Kafka интеграции
    if kafka_integration:
        kafka_integration.stop()
        logger.info("[KAFKA] Kafka интеграция остановлена")
    
    if kafka_task and not kafka_task.done():
        kafka_task.cancel()
        try:
            await kafka_task
        except asyncio.CancelledError:
            pass


# ========== FASTAPI APP ==========

app = FastAPI(
    title="LCT AI Service",
    description="Единый сервис для генерации пайплайнов миграции данных с поддержкой Hadoop и Spark",
    version="2.0.0",
    lifespan=lifespan
)


# ========== API ENDPOINTS ==========

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    kafka_status = "active" if kafka_integration else "disabled"
    
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        version="2.0.0",
        components={
            "lct_service": "active",
            "hadoop_integration": "active", 
            "spark_support": "active",
            "new_format": "active",
            "callback_support": "active",
            "kafka_integration": kafka_status
        }
    )


@app.post("/generate-pipeline")
async def generate_pipeline(
    request: PipelineRequest,
    background_tasks: BackgroundTasks
):
    """
    Главный endpoint для генерации пайплайнов
    Поддерживает новый формат из fix.md
    """
    start_time = datetime.now()
    
    try:
        logger.info(f"[REQUEST] Получен запрос {request.request_uid}")
        logger.info(f"[CONFIG] Spark режим: {request.spark_enabled}")
        logger.info(f"[DATA] Источников: {len(request.sources)}, Целей: {len(request.targets)}")
        
        # Выбор сервиса в зависимости от настроек
        service = lct_service_spark if request.spark_enabled else lct_service_standard
        
        # Преобразование запроса в внутренний формат
        request_data = {
            "request_uid": request.request_uid,
            "pipeline_uid": request.pipeline_uid,
            "timestamp": request.timestamp,
            "sources": request.sources,
            "targets": request.targets
        }
        
        # Обработка запроса
        result = service.process_new_format_request(request_data)
        
        # Расчет времени обработки
        processing_time = (datetime.now() - start_time).total_seconds()
        result["processing_time"] = processing_time
        
        # Логирование результатов
        logger.info(f"[SUCCESS] Запрос {request.request_uid} обработан за {processing_time:.2f}с")
        logger.info(f"[RESULT] Создано пайплайнов: {len(result.get('pipelines', []))}")
        
        if request.spark_enabled:
            spark_functions = result.get('spark_distributed_functions', 0)
            logger.info(f"[SPARK] Функций: {spark_functions}")
        
        # Отправка callback уведомления в фоне
        if request.callback_url:
            background_tasks.add_task(
                send_callback_notification, 
                request.callback_url, 
                result
            )
        
        return JSONResponse(
            status_code=200,
            content=result
        )
        
    except Exception as e:
        processing_time = (datetime.now() - start_time).total_seconds()
        
        logger.error(f"[ERROR] Ошибка обработки {request.request_uid}: {e}")
        
        error_result = {
            "status": "ERROR",
            "message": f"Ошибка обработки: {str(e)}",
            "request_uid": request.request_uid,
            "pipeline_uid": request.pipeline_uid,
            "timestamp": datetime.now().isoformat(),
            "processing_time": processing_time,
            "template": "",
            "pipelines": []
        }
        
        # Callback даже при ошибке
        if request.callback_url:
            background_tasks.add_task(
                send_callback_notification,
                request.callback_url, 
                error_result
            )
        
        return JSONResponse(
            status_code=500,
            content=error_result
        )


@app.post("/test-new-format")
async def test_new_format():
    """
    Тестовый endpoint для проверки нового формата
    Создает тестовый запрос и обрабатывает его
    """
    # Создание тестового запроса в новом формате
    test_request = {
        "request_uid": str(uuid.uuid4()),
        "pipeline_uid": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "sources": [
            {
                "$type": "csv",
                "parameters": {
                    "delimiter": ";",
                    "type": "csv",
                    "file_path": "C:\\Projects\\test_data.csv"
                },
                "schema_infos": [
                    {
                        "column_count": 3,
                        "columns": [
                            {
                                "name": "id",
                                "path": "id", 
                                "data_type": "integer",
                                "nullable": False
                            },
                            {
                                "name": "name",
                                "path": "name",
                                "data_type": "string", 
                                "nullable": True
                            },
                            {
                                "name": "amount",
                                "path": "amount",
                                "data_type": "float",
                                "nullable": True
                            }
                        ]
                    }
                ]
            }
        ],
        "targets": [
            {
                "$type": "postgre_s_q_l",
                "parameters": {
                    "type": "postgre_s_q_l",
                    "host": "localhost",
                    "port": 5432,
                    "database": "test_db",
                    "user": "postgres", 
                    "password": "password",
                    "schema": "public"
                }
            }
        ]
    }
    
    # Тестирование без Spark
    result_standard = lct_service_standard.process_new_format_request(test_request)
    
    # Тестирование с Spark
    result_spark = lct_service_spark.process_new_format_request(test_request)
    
    return {
        "test_completed": True,
        "standard_mode": {
            "status": result_standard.get("status"),
            "pipelines_count": len(result_standard.get("pipelines", [])),
            "spark_enabled": result_standard.get("spark_distribution_enabled", False)
        },
        "spark_mode": {
            "status": result_spark.get("status"),
            "pipelines_count": len(result_spark.get("pipelines", [])),
            "spark_enabled": result_spark.get("spark_distribution_enabled", False),
            "spark_functions": result_spark.get("spark_distributed_functions", 0)
        },
        "format_compliance": {
            "has_template": "template" in result_standard,
            "has_pipelines": "pipelines" in result_standard,
            "pipeline_structure_valid": all(
                all(field in pipeline for field in ["level", "id", "from", "to", "function_name", "function_body"])
                for pipeline in result_standard.get("pipelines", [])
            )
        }
    }


@app.get("/")
async def root():
    """Корневой endpoint с информацией о сервисе"""
    return {
        "service": "LCT AI Service",
        "version": "2.0.0",
        "description": "Единый сервис для генерации пайплайнов миграции данных",
        "features": [
            "Новый формат запросов ($type, parameters, schema_infos)",
            "Hadoop интеграция с автоконвертацией путей",
            "Template + pipelines формат ответа",
            "Опциональные Spark функции",
            "Callback уведомления",
            "Полное соответствие fix.md требованиям"
        ],
        "endpoints": {
            "/health": "Health check",
            "/generate-pipeline": "Основной endpoint для генерации пайплайнов",
            "/test-new-format": "Тестирование нового формата",
            "/docs": "API документация"
        }
    }


# ========== MAIN ==========

if __name__ == "__main__":
    print("[LAUNCH] Запуск LCT AI Service...")
    print("[INFO] Все требования fix.md реализованы:")
    print("   [OK] Новый формат запросов")
    print("   [OK] Hadoop пути")  
    print("   [OK] Template + pipelines ответ")
    print("   [OK] Spark функции")
    print("   [OK] Callback поддержка")
    print("   [OK] JSON конфигурация")
    print()
    print(f"[SERVER] Сервис будет доступен на http://{config.api_host}:{config.api_port}")
    print(f"[DOCS] API документация: http://{config.api_host}:{config.api_port}/docs")
    print()
    
    uvicorn.run(
        "main_service:app",
        host=config.api_host,
        port=config.api_port,
        reload=get_config('api.reload', False),
        log_level=config.log_level.lower()
    )