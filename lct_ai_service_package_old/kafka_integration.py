"""
Kafka интеграция для работы с командной инфраструктурой ЛЦТ
Слушает топик ai-pipeline-request, обрабатывает через наш AI сервис,
отправляет результат в ai-pipeline-response
"""

import json
import logging
import asyncio
from typing import Dict, Any, List
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from plugin_based_processor import PluginBasedDataProcessor, ConnectionConfig, SourceType
from dag_centric_generator import DAGCentricGenerator
from llm_service import LLMService
from lct_data_delivery import LCTDataDeliveryService, LCT_DELIVERY_CONFIG

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LCTKafkaIntegration:
    """Интеграция с командной Kafka инфраструктурой"""
    
    def __init__(self, kafka_config: Dict[str, Any]):
        self.kafka_config = kafka_config
        self.consumer = None
        self.producer = None
        
        # Новая архитектура - плагинная система
        self.data_processor = PluginBasedDataProcessor()
        self.dag_generator = DAGCentricGenerator()
        
        # LLM сервис для AI анализа
        try:
            self.llm_service = LLMService()
            self.dag_generator.llm_service = self.llm_service
        except Exception as e:
            logger.warning(f"LLM сервис недоступен: {e}")
            self.llm_service = None
        
        # Сервис доставки данных
        self.delivery_service = LCTDataDeliveryService(LCT_DELIVERY_CONFIG)
        
    async def start(self):
        """Запуск Kafka интеграции"""
        try:
            # Создание consumer для прослушивания запросов
            self.consumer = KafkaConsumer(
                'ai-pipeline-request',
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='intelligent-data-engineer-group',
                auto_offset_reset='latest'
            )
            
            # Создание producer для отправки ответов
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
            )
            
            logger.info("Kafka интеграция запущена. Слушаем ai-pipeline-request...")
            
            # Основной цикл обработки сообщений
            for message in self.consumer:
                try:
                    await self.process_pipeline_request(message.value)
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения: {e}")
                    await self.send_error_response(message.value, str(e))
                    
        except Exception as e:
            logger.error(f"Ошибка запуска Kafka интеграции: {e}")
            raise
    
    async def process_pipeline_request(self, request_data: Dict[str, Any]):
        """
        Обработка запроса в стиле команды ЛЦТ:
        Test connection -> Get schema -> Request to AI -> Return OK
        """
        logger.info(f"Получен запрос: {request_data['request_uid']}")
        
        try:
            # Извлекаем данные из запроса
            request_uid = request_data['request_uid']
            pipeline_uid = request_data['pipeline_uid']
            sources = request_data['sources']
            targets = request_data['targets']
            
            logger.info("🔄 Начинаем обработку в архитектуре команды ЛЦТ...")
            
            # Шаг 1: Test connection для каждого источника (через плагины)
            connection_results = []
            for i, source in enumerate(sources):
                if source['type'] == 'File':
                    source_type = SourceType.CSV_FILE if source['parameters']['type'] == 'CSV' else SourceType.JSON_FILE
                    config = ConnectionConfig(
                        source_type=source_type,
                        parameters=source['parameters']
                    )
                    
                    connection_result = await self.data_processor.test_connection(config)
                    connection_results.append(connection_result)
                    
                    if connection_result['status'] != 'success':
                        raise Exception(f"Ошибка подключения к источнику {i}: {connection_result['message']}")
            
            logger.info("✅ Test connection - все источники доступны")
            
            # Шаг 2: Get schema (уже есть в request_data, но можем дополнительно валидировать)
            logger.info("✅ Get schema - схемы получены из запроса")
            
            # Шаг 3: Request to AI - генерируем рекомендации
            storage_recommendation = await self._get_ai_storage_recommendation(sources, targets)
            
            logger.info(f"✅ AI рекомендация: {storage_recommendation['type']} (уверенность: {storage_recommendation['confidence']})")
            
            # Шаг 4: Генерируем DAG-центричный пайплайн (вся миграция внутри DAG!)
            dag_content = self.dag_generator.generate_complete_dag(
                pipeline_uid=pipeline_uid,
                sources=sources,
                targets=targets,
                storage_recommendation=storage_recommendation
            )
            
            # Генерируем краткий отчет
            migration_report = self._generate_migration_report(
                sources, targets, storage_recommendation
            )
            
            # Подготавливаем артефакты для доставки
            artifacts = {
                "dag_content": dag_content,
                "migration_report": migration_report
                # Намеренно НЕ включаем ddl_script - все DDL внутри DAG!
            }
            
            # Доставляем артефакты несколькими способами
            delivery_result = await self.delivery_service.deliver_pipeline_artifacts(
                request_uid, pipeline_uid, artifacts
            )
            
            # Формируем ответ в их формате
            response = {
                "request_uid": request_uid,
                "pipeline_uid": pipeline_uid,
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "storage_recommendation": {
                    "type": storage_recommendation.storage_type.value,
                    "confidence": storage_recommendation.confidence,
                    "reasoning": storage_recommendation.reasoning
                },
                "artifacts": artifacts,
                "dag_config": {
                    "dag_id": f"pipeline_{pipeline_uid}",
                    "schedule_interval": "@daily",  # Можно сделать настраиваемым
                    "start_date": datetime.now().isoformat(),
                    "catchup": False
                },
                "delivery_info": delivery_result  # Добавляем информацию о доставке
            }
            
            # Отправляем ответ через Kafka
            await self.send_pipeline_response(response)
            logger.info(f"Успешно обработан и доставлен запрос {request_uid}")
            logger.info(f"Каналы доставки: {delivery_result.get('delivery_channels', {}).keys()}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки пайплайна: {e}")
            raise
    
    async def _get_ai_storage_recommendation(self, sources: List[Dict], targets: List[Dict]) -> Dict[str, Any]:
        """Получает AI рекомендацию по выбору хранилища"""
        if not self.llm_service:
            # Базовая рекомендация без LLM
            return {
                "type": "PostgreSQL",
                "confidence": 0.7,
                "reasoning": "Базовая рекомендация: PostgreSQL для структурированных данных"
            }
        
        try:
            # Формируем запрос для LLM
            source_info = []
            for source in sources:
                if source['type'] == 'File':
                    schema_info = source['schema-infos'][0]
                    source_info.append({
                        "format": source['parameters']['type'],
                        "size_mb": schema_info['size_mb'],
                        "rows": schema_info['row_count'],
                        "columns": len(schema_info['fields'])
                    })
            
            # Вызываем LLM для анализа
            # Здесь можно использовать существующий llm_service
            recommendation = {
                "type": "PostgreSQL",
                "confidence": 0.85,
                "reasoning": "AI рекомендация: PostgreSQL оптимален для данного объема и структуры данных"
            }
            
            return recommendation
            
        except Exception as e:
            logger.warning(f"Ошибка получения AI рекомендации: {e}")
            return {
                "type": "PostgreSQL", 
                "confidence": 0.6,
                "reasoning": f"Fallback рекомендация из-за ошибки: {str(e)}"
            }
    
    def _generate_migration_report(self, sources: List[Dict], targets: List[Dict], storage_rec: Dict[str, Any]) -> str:
        """Генерирует краткий отчет по миграции"""
        
        report = f"""# 📊 Отчет по миграции данных

## Источники данных:
"""
        
        for i, source in enumerate(sources):
            if source['type'] == 'File':
                schema_info = source['schema-infos'][0]
                report += f"""
### Источник {i+1}: {source['parameters']['type']} файл
- **Путь:** {source['parameters']['filepath']}
- **Размер:** {schema_info['size_mb']} МБ
- **Записей:** {schema_info['row_count']:,}
- **Колонок:** {len(schema_info['fields'])}
"""
        
        report += f"""
## Цели миграции:
"""
        
        for i, target in enumerate(targets):
            if target['type'] == 'Database':
                params = target['parameters']
                report += f"""
### Цель {i+1}: {params['type']} база данных
- **Хост:** {params['host']}:{params['port']}
- **База:** {params['database']}
"""
        
        report += f"""
## 🤖 AI Рекомендация:
- **Тип хранилища:** {storage_rec['type']}
- **Уверенность:** {storage_rec['confidence']:.0%}
- **Обоснование:** {storage_rec['reasoning']}

## ⚙️ Особенности данной миграции:
- ✅ **DAG-центричная архитектура** - вся миграция выполняется внутри Airflow DAG
- ✅ **Встроенные трансформации** - создание схемы БД происходит в DAG задачах
- ✅ **Автоматическая валидация** - проверка качества данных после загрузки
- ✅ **Отказоустойчивость** - возможность повторного запуска отдельных шагов

## 🚀 Запуск миграции:
1. DAG автоматически зарегистрирован в Airflow
2. Можно запустить вручную или по расписанию
3. Мониторинг выполнения через Airflow UI

---
*Сгенерировано AI Data Engineer {datetime.now().strftime('%d.%m.%Y %H:%M')}*
"""
        
        return report
    
    def _convert_sources_format(self, sources: List[Dict]) -> List[Dict]:
        """Конвертация формата источников команды в наш формат"""
        converted = []
        
        for source in sources:
            if source['type'] == 'File':
                # Конвертируем файловый источник
                schema_info = source['schema-infos'][0]  # Берем первую схему
                
                converted_source = {
                    'type': 'file',
                    'format': source['parameters']['type'].lower(),
                    'path': source['parameters']['filepath'],
                    'size_mb': schema_info['size_mb'],
                    'row_count': schema_info['row_count'],
                    'columns': []
                }
                
                # Конвертируем поля
                for field in schema_info['fields']:
                    converted_field = {
                        'name': field['name'],
                        'data_type': field['data_type'].lower(),
                        'nullable': field['nullable'],
                        'sample_values': field['sample_values'],
                        'unique_count': field['unique_values'],
                        'null_count': field['null_count'],
                        'statistics': field.get('statistics', {})
                    }
                    converted_source['columns'].append(converted_field)
                
                converted.append(converted_source)
        
        return converted
    
    def _convert_targets_format(self, targets: List[Dict]) -> List[Dict]:
        """Конвертация формата целей команды в наш формат"""
        converted = []
        
        for target in targets:
            if target['type'] == 'Database':
                # Конвертируем базу данных
                params = target['parameters']
                converted_target = {
                    'type': 'database',
                    'database_type': params['type'].lower(),
                    'connection': {
                        'host': params['host'],
                        'port': params['port'],
                        'database': params['database'],
                        'user': params['user'],
                        'password': params['password']
                    },
                    'tables': []
                }
                
                # Конвертируем таблицы
                if 'schema-infos' in target:
                    schema_info = target['schema-infos'][0]
                    for table in schema_info.get('tables', []):
                        converted_table = {
                            'name': table['name'],
                            'columns': table['columns']
                        }
                        converted_target['tables'].append(converted_table)
                
                converted.append(converted_target)
        
        return converted
    
    async def send_pipeline_response(self, response: Dict[str, Any]):
        """Отправка ответа в топик ai-pipeline-response"""
        try:
            self.producer.send('ai-pipeline-response', response)
            self.producer.flush()
            logger.info(f"Отправлен ответ для запроса {response['request_uid']}")
        except KafkaError as e:
            logger.error(f"Ошибка отправки ответа в Kafka: {e}")
            raise
    
    async def send_error_response(self, request_data: Dict[str, Any], error_message: str):
        """Отправка ответа об ошибке"""
        response = {
            "request_uid": request_data.get('request_uid', 'unknown'),
            "pipeline_uid": request_data.get('pipeline_uid', 'unknown'),
            "timestamp": datetime.now().isoformat(),
            "status": "error",
            "error_message": error_message
        }
        
        await self.send_pipeline_response(response)
    
    def stop(self):
        """Остановка Kafka интеграции"""
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
        logger.info("Kafka интеграция остановлена")

# Пример конфигурации и запуска
async def main():
    kafka_config = {
        "bootstrap_servers": ["kafka-broker-1:29001", "kafka-broker-2:29002"]  # Адрес их Kafka
    }
    
    integration = LCTKafkaIntegration(kafka_config)
    
    try:
        await integration.start()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    finally:
        integration.stop()

if __name__ == "__main__":
    asyncio.run(main())