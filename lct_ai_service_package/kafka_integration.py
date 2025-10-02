"""
Kafka интеграция для работы с командной инфраструктурой ЛЦТ
Упрощенная версия для интеграции с main_service.py
"""

import json
import logging
import asyncio
from typing import Dict, Any, List
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from config_loader import config
from updated_lct_service import UpdatedLCTAIService

logger = logging.getLogger(__name__)

class LCTKafkaIntegration:
    """Интеграция с командной Kafka инфраструктурой"""
    
    def __init__(self, kafka_config: Dict[str, Any]):
        self.kafka_config = kafka_config
        self.consumer = None
        self.producer = None
        self.running = False
        
        # Используем существующий сервис
        self.ai_service = UpdatedLCTAIService()
        
    async def start(self):
        """Запуск Kafka интеграции"""
        if not self.kafka_config.get('enabled', False):
            logger.info("Kafka интеграция отключена в конфигурации")
            return
            
        try:
            # Создание consumer для прослушивания запросов
            self.consumer = KafkaConsumer(
                self.kafka_config['topics']['request'],
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id=self.kafka_config['consumer_group_id'],
                auto_offset_reset=self.kafka_config['auto_offset_reset'],
                consumer_timeout_ms=self.kafka_config.get('timeout', 10) * 1000
            )
            
            # Создание producer для отправки ответов
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
            )
            
            self.running = True
            logger.info(f"Kafka интеграция запущена. Слушаем топик: {self.kafka_config['topics']['request']}")
            
            # Основной цикл обработки сообщений
            while self.running:
                try:
                    for message in self.consumer:
                        if not self.running:
                            break
                        try:
                            await self.process_pipeline_request(message.value)
                        except Exception as e:
                            logger.error(f"Ошибка обработки сообщения: {e}")
                            await self.send_error_response(message.value, str(e))
                except Exception as e:
                    logger.error(f"Ошибка в цикле обработки Kafka: {e}")
                    await asyncio.sleep(5)  # Пауза перед повторной попыткой
                    
        except Exception as e:
            logger.error(f"Ошибка запуска Kafka интеграции: {e}")
            raise
    
    async def process_pipeline_request(self, request_data: Dict[str, Any]):
        """Обработка запроса через существующий AI сервис"""
        logger.info(f"Получен Kafka запрос: {request_data.get('request_uid', 'unknown')}")
        
        try:
            # Конвертируем запрос в наш формат
            converted_request = self._convert_request_format(request_data)
            
            # Обрабатываем через существующий AI сервис
            result = self.ai_service.process_new_format_request(converted_request)
            
            # Формируем ответ в формате команды
            response = {
                "request_uid": request_data['request_uid'],
                "pipeline_uid": request_data['pipeline_uid'],
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "storage_recommendation": result.get('storage_recommendation', {}),
                "dag_content": result.get('dag_content', ''),
                "artifacts": {
                    "dag_content": result.get('dag_content', ''),
                    "migration_report": result.get('migration_report', '')  # ИСПРАВЛЕНО: migration_report вместо summary
                }
            }
            
            # Отправляем ответ через Kafka
            self.send_pipeline_response(response)
            logger.info(f"Успешно обработан Kafka запрос {request_data['request_uid']}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки Kafka запроса: {e}")
            self.send_error_response(request_data, str(e))
            raise
    
    def _convert_request_format(self, kafka_request: Dict[str, Any]) -> Dict[str, Any]:
        """Конвертация запроса Kafka в формат нашего AI сервиса"""
        return {
            "request_uid": kafka_request['request_uid'],
            "pipeline_uid": kafka_request['pipeline_uid'],
            "timestamp": kafka_request.get('timestamp', datetime.now().isoformat()),
            "sources": kafka_request['sources'],
            "targets": kafka_request['targets'],
            "spark_enabled": kafka_request.get('spark_enabled', False),
            "callback_url": None  # Не используем callback для Kafka
        }
    
    def send_pipeline_response(self, response: Dict[str, Any]):
        """Отправка ответа в топик response"""
        try:
            self.producer.send(self.kafka_config['topics']['response'], response)
            self.producer.flush()
            logger.info(f"Отправлен ответ в Kafka для запроса {response['request_uid']}")
        except KafkaError as e:
            logger.error(f"Ошибка отправки ответа в Kafka: {e}")
            raise
    
    def send_error_response(self, request_data: Dict[str, Any], error_message: str):
        """Отправка ответа об ошибке"""
        response = {
            "request_uid": request_data.get('request_uid', 'unknown'),
            "pipeline_uid": request_data.get('pipeline_uid', 'unknown'),
            "timestamp": datetime.now().isoformat(),
            "status": "error",
            "error_message": error_message
        }
        
        self.send_pipeline_response(response)
    
    def stop(self):
        """Остановка Kafka интеграции"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
        logger.info("Kafka интеграция остановлена")

# Функция для запуска в фоновом режиме
async def start_kafka_integration_background(kafka_config: Dict[str, Any]):
    """Запуск Kafka интеграции в фоновом режиме"""
    if not kafka_config.get('enabled', False):
        logger.info("Kafka интеграция отключена")
        return None
        
    integration = LCTKafkaIntegration(kafka_config)
    
    # Запускаем в фоновой задаче
    task = asyncio.create_task(integration.start())
    logger.info("Kafka интеграция запущена в фоновом режиме")
    
    return integration, task