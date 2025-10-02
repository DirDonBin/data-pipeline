import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import threading
import time

from config_loader import get_config, load_config
from updated_lct_service import UpdatedLCTService

logger = logging.getLogger(__name__)

class KafkaIntegration:
    """Интеграция с Kafka для обработки запросов пайплайнов"""
    
    def __init__(self):
        self.kafka_config = get_config("kafka", {})
        self.lct_service = UpdatedLCTService()
        
        self.producer = None
        self.consumer = None
        self.is_running = False
        
        logger.info("KafkaIntegration инициализирован")
    
    def connect(self) -> bool:
        """Подключение к Kafka"""
        try:
            if not self.kafka_config.get("enabled", False):
                logger.warning("Kafka интеграция отключена в конфигурации")
                return False
            
            # Настройка producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config.get("servers", ["localhost:9092"]),
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                acks='all',
                retries=3,
                retry_backoff_ms=1000
            )
            
            # Настройка consumer
            self.consumer = KafkaConsumer(
                self.kafka_config.get("input_topic", "pipeline-requests"),
                bootstrap_servers=self.kafka_config.get("servers", ["localhost:9092"]),
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                group_id=self.kafka_config.get("group_id", "lct-ai-service"),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            
            logger.info("Подключение к Kafka успешно установлено")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка подключения к Kafka: {e}")
            return False
    
    def start_consuming(self):
        """Запуск потребления сообщений из Kafka"""
        if not self.connect():
            logger.error("Не удалось подключиться к Kafka")
            return
        
        self.is_running = True
        logger.info("Запуск потребления сообщений из Kafka...")
        
        try:
            for message in self.consumer:
                if not self.is_running:
                    break
                
                logger.info(f"Получено сообщение из Kafka: {message.topic}:{message.partition}:{message.offset}")
                
                try:
                    # Обработка запроса
                    request_data = message.value
                    response = self.process_pipeline_request(request_data)
                    
                    # Отправка ответа
                    self.send_response(response, message.key)
                    
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения: {e}")
                    # Отправка ошибки в ответ
                    error_response = {
                        "uid": request_data.get("uid", "unknown") if 'request_data' in locals() else "unknown",
                        "status": "error",
                        "error": str(e),
                        "migration_report": f"Ошибка обработки запроса: {str(e)}"
                    }
                    self.send_response(error_response, message.key)
                    
        except Exception as e:
            logger.error(f"Критическая ошибка в consumer: {e}")
        finally:
            self.stop()
    
    def process_pipeline_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Обработка запроса пайплайна"""
        try:
            logger.info(f"Обработка запроса пайплайна: {request_data.get('uid', 'unknown')}")
            
            # Валидация входных данных
            if not isinstance(request_data, dict):
                raise ValueError("Неверный формат данных запроса")
            
            # Обработка через LCT Service
            result = self.lct_service.process_new_format_request(request_data)
            
            # ИСПРАВЛЕНО: Используем правильное поле migration_report
            response = {
                "uid": result.get("uid", "unknown"),
                "pipelineUID": result.get("pipelineUID", "unknown"),
                "status": result.get("status", "success"),
                "created_at": result.get("created_at", ""),
                "template": result.get("template", ""),
                "pipelines": result.get("pipelines", []),
                "migration_report": result.get("migration_report", ""),  # ИСПРАВЛЕНО: было 'summary'
                "metadata": result.get("metadata", {})
            }
            
            logger.info(f"Запрос успешно обработан: {response['uid']}")
            logger.info(f"Migration report размер: {len(response['migration_report'])} символов")
            
            return response
            
        except Exception as e:
            logger.error(f"Ошибка обработки запроса пайплайна: {e}")
            raise
    
    def send_response(self, response: Dict[str, Any], message_key: Optional[str] = None):
        """Отправка ответа в Kafka"""
        try:
            output_topic = self.kafka_config.get("output_topic", "pipeline-responses")
            
            # Отправка сообщения
            future = self.producer.send(
                output_topic,
                value=response,
                key=message_key
            )
            
            # Ожидание подтверждения
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Ответ отправлен в Kafka: {output_topic}:{record_metadata.partition}:{record_metadata.offset}")
            
        except Exception as e:
            logger.error(f"Ошибка отправки ответа в Kafka: {e}")
            raise
    
    def send_pipeline_request(self, request_data: Dict[str, Any], 
                            key: Optional[str] = None) -> bool:
        """Отправка запроса пайплайна в Kafka"""
        try:
            if not self.producer:
                if not self.connect():
                    return False
            
            input_topic = self.kafka_config.get("input_topic", "pipeline-requests")
            
            # Отправка сообщения
            future = self.producer.send(
                input_topic,
                value=request_data,
                key=key
            )
            
            # Ожидание подтверждения
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Запрос отправлен в Kafka: {input_topic}:{record_metadata.partition}:{record_metadata.offset}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки запроса в Kafka: {e}")
            return False
    
    def stop(self):
        """Остановка Kafka интеграции"""
        self.is_running = False
        
        try:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer остановлен")
            
            if self.producer:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer остановлен")
                
        except Exception as e:
            logger.error(f"Ошибка остановки Kafka: {e}")
    
    def get_kafka_status(self) -> Dict[str, Any]:
        """Получение статуса Kafka подключения"""
        try:
            status = {
                "enabled": self.kafka_config.get("enabled", False),
                "connected": self.producer is not None and self.consumer is not None,
                "running": self.is_running,
                "config": {
                    "servers": self.kafka_config.get("servers", []),
                    "input_topic": self.kafka_config.get("input_topic", ""),
                    "output_topic": self.kafka_config.get("output_topic", ""),
                    "group_id": self.kafka_config.get("group_id", "")
                }
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Ошибка получения статуса Kafka: {e}")
            return {"status": "error", "error": str(e)}
    
    def test_connection(self) -> Dict[str, Any]:
        """Тестирование подключения к Kafka"""
        try:
            if not self.kafka_config.get("enabled", False):
                return {
                    "status": "disabled",
                    "message": "Kafka интеграция отключена в конфигурации"
                }
            
            # Попытка подключения
            test_producer = KafkaProducer(
                bootstrap_servers=self.kafka_config.get("servers", ["localhost:9092"]),
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=3000
            )
            
            # Получение метаданных для проверки подключения
            metadata = test_producer.list_topics(timeout=5)
            
            test_producer.close()
            
            return {
                "status": "success",
                "message": "Подключение к Kafka успешно",
                "topics": list(metadata.topics.keys())
            }
            
        except Exception as e:
            logger.error(f"Ошибка тестирования Kafka: {e}")
            return {
                "status": "error",
                "message": f"Ошибка подключения к Kafka: {str(e)}"
            }

def start_kafka_consumer_thread():
    """Запуск Kafka consumer в отдельном потоке"""
    kafka_integration = KafkaIntegration()
    
    def consumer_worker():
        try:
            kafka_integration.start_consuming()
        except Exception as e:
            logger.error(f"Ошибка в Kafka consumer потоке: {e}")
    
    consumer_thread = threading.Thread(target=consumer_worker, daemon=True)
    consumer_thread.start()
    logger.info("Kafka consumer запущен в отдельном потоке")
    
    return kafka_integration