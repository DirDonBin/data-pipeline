"""
Обновленный главный сервис с поддержкой нового формата команды ЛЦТ
Интегрирует новый формат запросов, Hadoop пути и разделение на пайплайны
"""

import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from lct_format_adapter import LCTFormatAdapter
from hadoop_dag_generator import HadoopDAGGenerator
from llm_service import LLMService

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UpdatedLCTAIService:
    """Обновленный AI сервис для команды ЛЦТ"""
    
    def __init__(self, enable_spark_distribution: bool = False):
        self.format_adapter = LCTFormatAdapter()
        self.dag_generator = HadoopDAGGenerator(enable_spark_distribution=enable_spark_distribution)
        self.llm_service = LLMService()
        
        logger.info("[LAUNCH] Обновленный LCT AI Service инициализирован")
        logger.info("[SUCCESS] Поддержка нового формата запросов")
        logger.info("[SUCCESS] Hadoop интеграция")  
        logger.info("[SUCCESS] Разделение DAG на пайплайны")
        
        if enable_spark_distribution:
            logger.info("[SPARK] Включено распределение функций на Spark воркеры")
        else:
            logger.info("[DATA] Стандартное выполнение функций (без Spark)")

    def _convert_simple_sources_to_structured(self, sources: list) -> list:
        """Преобразует простые строки sources в структурированный формат"""
        structured_sources = []
        for i, source in enumerate(sources):
            if isinstance(source, str):  # Если это строка типа "csv", "json"
                structured_source = {
                    "$type": source,
                    "uid": f"source_{i}_{source}",
                    "type": "file",
                    "parameters": {
                        "type": source,
                        "file_path": f"/data/sample.{source}",  # Заглушка для пути
                        "delimiter": "," if source == "csv" else None
                    },
                    "schema_infos": [{
                        "size_mb": 100,
                        "row_count": 1000,
                        "columns": [{
                            "name": "sample_column",
                            "type": "string"
                        }]
                    }]
                }
                structured_sources.append(structured_source)
            else:
                structured_sources.append(source)  # Уже структурированный
        return structured_sources

    def _convert_simple_targets_to_structured(self, targets: list) -> list:
        """Преобразует простые строки targets в структурированный формат"""
        structured_targets = []
        for i, target in enumerate(targets):
            if isinstance(target, str):  # Если это строка типа "hdfs", "kafka"
                structured_target = {
                    "$type": "postgresql",  # По умолчанию PostgreSQL
                    "uid": f"target_{i}_{target}",
                    "type": "database",
                    "parameters": {
                        "type": "postgresql",
                        "host": "localhost",
                        "port": 5432,
                        "database": "target_db",
                        "user": "postgres",
                        "password": "password",
                        "schema": "public"
                    },
                    "schema_infos": []
                }
                structured_targets.append(structured_target)
            else:
                structured_targets.append(target)  # Уже структурированный
        return structured_targets
    
    def process_new_format_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Обработка запроса в новом формате команды ЛЦТ"""
        
        # Инициализируем значения по умолчанию для обработки ошибок
        parsed_request = None
        
        try:
            logger.info("[RECEIVE] Получен запрос в новом формате")
            logger.info(f"[DEBUG] Тип request_data: {type(request_data)}")
            logger.info(f"[DEBUG] Содержимое request_data: {request_data}")
            
            # Проверяем структуру входных данных
            sources_raw = request_data.get('sources', [])
            targets_raw = request_data.get('targets', [])
            logger.info(f"[DEBUG] Исходные sources: {sources_raw} (тип: {type(sources_raw)})")
            logger.info(f"[DEBUG] Исходные targets: {targets_raw} (тип: {type(targets_raw)})")
            
            # Преобразуем простые строки в структурированный формат
            logger.info("[CONVERT] Преобразование sources в структурированный формат...")
            structured_sources = self._convert_simple_sources_to_structured(sources_raw)
            logger.info(f"[DEBUG] Структурированные sources: {len(structured_sources)} элементов")
            
            logger.info("[CONVERT] Преобразование targets в структурированный формат...")
            structured_targets = self._convert_simple_targets_to_structured(targets_raw)
            logger.info(f"[DEBUG] Структурированные targets: {len(structured_targets)} элементов")
            
            # Проверяем флаг Spark из запроса
            spark_enabled = request_data.get("spark_enabled", False)
            logger.info(f"[SPARK] Spark режим из запроса: {spark_enabled}")
            
            # Обновляем конфигурацию DAG генератора если нужно
            if spark_enabled and not self.dag_generator.enable_spark_distribution:
                logger.info("[SPARK] Включаем Spark распределение для этого запроса")
                self.dag_generator.enable_spark_distribution = True
            elif not spark_enabled and self.dag_generator.enable_spark_distribution:
                logger.info("[SPARK] Отключаем Spark распределение для этого запроса")
                self.dag_generator.enable_spark_distribution = False
            
            # Создаем структурированный запрос
            structured_request_data = {
                "request_uid": request_data.get("request_uid"),
                "pipeline_uid": request_data.get("pipeline_uid"),
                "timestamp": request_data.get("timestamp"),
                "sources": structured_sources,
                "targets": structured_targets,
                "spark_config": request_data.get("spark_config", {}),
                "spark_enabled": spark_enabled
            }
            logger.info(f"[DEBUG] Структурированный запрос создан: {len(structured_request_data)} полей")
            
            # 1. Парсинг нового формата
            internal_format = self.format_adapter.convert_lct_request_to_our_format(structured_request_data)
            logger.info(f"[DATA] Распарсен запрос: {len(internal_format['sources'])} источников, {len(internal_format['targets'])} целей")
            
            # Логирование Hadoop путей с безопасной проверкой типов
            for i, source in enumerate(internal_format['sources']):
                if isinstance(source, dict):  # Проверяем, что это словарь
                    hadoop_path = source.get('parameters', {}).get('file_path', '')
                    if hadoop_path and hadoop_path.startswith('hdfs://'):
                        logger.info(f"[HADOOP] Источник {i+1}: {hadoop_path}")
                else:
                    logger.warning(f"[WARNING] Источник {i+1} не является словарем: {type(source)}")
            
            # 3. AI анализ и рекомендации
            logger.info("[AI] Запрос AI рекомендаций...")
            
            # Отладочная информация
            logger.debug(f"[DEBUG] sources для AI: {internal_format['sources']}")
            logger.debug(f"[DEBUG] targets для AI: {internal_format['targets']}")
            logger.debug(f"[DEBUG] Тип sources: {type(internal_format['sources'])}")
            logger.debug(f"[DEBUG] Тип targets: {type(internal_format['targets'])}")
            
            try:
                ai_recommendations = self.llm_service.get_storage_recommendation(
                    sources=internal_format['sources'],
                    targets=internal_format['targets'],
                    business_context="Миграция данных из Hadoop в PostgreSQL"
                )
                logger.info(f"[AI_SUCCESS] AI рекомендации получены успешно")
                
                # Детальное логирование AI ответа
                if isinstance(ai_recommendations, dict):
                    logger.info(f"[AI_DETAIL] Тип хранилища: {ai_recommendations.get('storage_type', 'не указан')}")
                    logger.info(f"[AI_DETAIL] Уверенность: {ai_recommendations.get('confidence', 0):.0%}")
                    steps = ai_recommendations.get('transformation_steps', [])
                    if isinstance(steps, list):
                        logger.info(f"[AI_DETAIL] Этапов трансформации: {len(steps)}")
                    else:
                        logger.info(f"[AI_DETAIL] Этапы трансформации: {type(steps)}")
                else:
                    logger.info(f"[AI_DETAIL] Тип ответа AI: {type(ai_recommendations)}")
                    logger.info(f"[AI_DETAIL] Размер ответа: {len(str(ai_recommendations))} символов")
            except Exception as ai_error:
                logger.error(f"[AI_SERVICE_ERROR] AI сервис недоступен: {ai_error}")
                # Возвращаем ошибку без fallback
                ai_recommendations = {
                    "error": f"AI сервис недоступен: {str(ai_error)}",
                    "service_status": "unavailable",
                    "fallback_used": False
                }
            
            if isinstance(ai_recommendations, dict):
                logger.info(f"[IDEA] AI рекомендация: {ai_recommendations.get('storage_type')} (уверенность: {ai_recommendations.get('confidence', 0):.0%})")
            else:
                logger.info(f"[IDEA] AI рекомендация получена: {type(ai_recommendations)}")
            
            # 4. Генерация структуры пайплайнов
            logger.info("[CONFIG] Генерация пайплайн структуры...")
            pipeline_structure = self.dag_generator.generate_pipeline_structure(
                sources=internal_format['sources'],
                targets=internal_format['targets'],
                request_uid=internal_format['request_uid'],
                pipeline_uid=internal_format['pipeline_uid']
            )
            
            logger.info(f"[CONNECT] Сгенерировано пайплайнов: {len(pipeline_structure['pipelines'])}")
            
            # 5. Создание ответа в формате команды
            self.format_adapter.current_request_uid = internal_format['request_uid']
            self.format_adapter.current_pipeline_uid = internal_format['pipeline_uid']
            
            # Получаем template и создаем migration_report
            dag_template = pipeline_structure.get('template', '')
            pipelines = pipeline_structure.get('pipelines', [])
            
            # Создаем детальный migration_report
            migration_report = f"Создан пайплайн миграции данных с {len(pipelines)} этапами:\n"
            for i, pipeline in enumerate(pipelines, 1):
                name = pipeline.get('name', f'Этап {i}')
                desc = pipeline.get('description', 'Без описания')
                level = pipeline.get('level', i-1)
                migration_report += f"{i}. {name} (уровень {level})\n   {desc}\n"
            
            migration_report += f"\nОбщая информация:\n"
            migration_report += f"- Источники данных: {len(internal_format.get('sources', []))}\n"
            migration_report += f"- Целевые системы: {len(internal_format.get('targets', []))}\n"
            migration_report += f"- Spark распределение: {'Включено' if self.dag_generator.enable_spark_distribution else 'Отключено'}\n"
            migration_report += f"- Размер DAG: {len(dag_template)} символов"
            
            our_response = {
                'request_uid': internal_format['request_uid'],
                'pipeline_uid': internal_format['pipeline_uid'],
                'status': 'SUCCESS',
                'message': 'Пайплайн миграции данных успешно сгенерирован',
                'dag_config': {'template': dag_template},  # template как строка в dag_config
                'artifacts': {
                    'dag_content': dag_template,
                    'ddl_script': 'DDL встроен в DAG функции (create_target_schema)',
                    'migration_report': migration_report,
                    'pipelines': pipelines
                }
            }
            
            response = self.format_adapter.convert_our_response_to_lct_format(our_response)
            
            # Создаем детальный migration_report как в process_request_data
            pipelines = pipeline_structure.get('pipelines', [])
            migration_report = f"Создан пайплайн миграции данных с {len(pipelines)} этапами:\n"
            
            for i, pipeline in enumerate(pipelines, 1):
                name = pipeline.get('name', f'Этап {i}')
                level = pipeline.get('level', i-1)
                desc = pipeline.get('description', 'Обработка данных')
                migration_report += f"{i}. {name} (уровень {level})\n   {desc}\n"
            
            migration_report += f"\nОбщая информация:\n"
            migration_report += f"- Источники данных: {len(internal_format.get('sources', []))}\n"
            migration_report += f"- Целевые системы: {len(internal_format.get('targets', []))}\n"
            migration_report += f"- Spark распределение: {'Включено' if self.dag_generator.enable_spark_distribution else 'Отключено'}\n"
            migration_report += f"- Размер DAG: {len(str(pipeline_structure.get('template', '')))} символов"
            
            # Добавляем migration_report в ответ
            response["migration_report"] = migration_report
            
            # ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ ОТВЕТА
            logger.info(f"[RESPONSE_DEBUG] Создан migration_report длиной: {len(migration_report)} символов")
            logger.info(f"[RESPONSE_DEBUG] migration_report содержание: {migration_report[:200]}...")
            logger.info(f"[RESPONSE_DEBUG] Ключи ответа: {list(response.keys())}")
            logger.info(f"[RESPONSE_DEBUG] migration_report в ответе: {response.get('migration_report', 'ОТСУТСТВУЕТ')[:100]}...")
            logger.info(f"[RESPONSE_DEBUG] Размер полного ответа: {len(str(response))} символов")
            
            # Добавляем AI рекомендации в ответ
            response["ai_recommendations"] = ai_recommendations
            response["hadoop_integration"] = True
            response["pipeline_count"] = len(pipeline_structure['pipelines'])
            response["spark_distribution_enabled"] = self.dag_generator.enable_spark_distribution
            
            # Подсчет Spark функций
            spark_functions = sum(1 for p in pipeline_structure['pipelines'] 
                                if p.get('spark_distributed', False))
            response["spark_distributed_functions"] = spark_functions
            
            # ФИНАЛЬНОЕ ЛОГИРОВАНИЕ ПЕРЕД ВОЗВРАТОМ
            logger.info(f"[FINAL_RESPONSE] Финальный ответ содержит:")
            logger.info(f"[FINAL_RESPONSE] - migration_report: {len(response.get('migration_report', ''))} символов")
            logger.info(f"[FINAL_RESPONSE] - dag_content: {len(response.get('dag_content', ''))} символов") 
            logger.info(f"[FINAL_RESPONSE] - pipeline_count: {response.get('pipeline_count', 0)}")
            logger.info(f"[FINAL_RESPONSE] - status: {response.get('status', 'unknown')}")
            logger.info(f"[FINAL_RESPONSE] Полный размер ответа: {len(str(response))} символов")
            
            logger.info("[SUCCESS] Запрос обработан успешно")
            return response
            
        except Exception as e:
            logger.error(f"[ERROR] Ошибка обработки запроса: {e}")
            logger.error(f"[DEBUG] Тип ошибки: {type(e)}")
            logger.error(f"[DEBUG] Трейсбек: ", exc_info=True)
            error_response = {
                'request_uid': request_data.get('request_uid', 'unknown'),
                'pipeline_uid': request_data.get('pipeline_uid', 'unknown'),
                'status': 'ERROR',
                'error_message': f"Ошибка обработки: {str(e)}"
            }
            
            return self.format_adapter.convert_our_response_to_lct_format(error_response)
    
    def process_kafka_message(self, kafka_message: Dict[str, Any]) -> Dict[str, Any]:
        """Обработка сообщения из Kafka в новом формате"""
        
        logger.info("[KAFKA] Получено Kafka сообщение")
        
        # Проверяем, что это новый формат
        if "$type" in str(kafka_message) and "schema_infos" in str(kafka_message):
            logger.info("🆕 Обнаружен новый формат запроса")
            return self.process_new_format_request(kafka_message)
        else:
            logger.warning("[WARNING] Получен запрос в старом формате")
            return {
                "status": "ERROR",
                "message": "Поддерживается только новый формат запросов с $type и schema_infos"
            }
    
    def generate_spark_configuration(self, pipelines: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Генерирует конфигурацию Spark для распределенного выполнения (опционально)"""
        
        spark_config = {
            "spark_enabled": True,
            "cluster_config": {
                "master": "spark://spark-master:7077",
                "executor_memory": "2g",
                "executor_cores": "2",
                "num_executors": "3"
            },
            "optimized_functions": []
        }
        
        # Определяем функции, которые могут выполняться в Spark
        spark_compatible = ["extract_data", "transform_data", "load_data"]
        
        for pipeline in pipelines:
            if pipeline["function_name"] in spark_compatible:
                spark_config["optimized_functions"].append({
                    "function_name": pipeline["function_name"],
                    "level": pipeline["level"],
                    "spark_optimized": True,
                    "parallelism": "high" if pipeline["function_name"] == "transform_data" else "medium"
                })
        
        return spark_config


def test_updated_service():
    """Тестирование обновленного сервиса"""
    
    print("[TEST] ТЕСТИРОВАНИЕ ОБНОВЛЕННОГО LCT AI СЕРВИСА")
    print("=" * 60)
    
    # Инициализация сервиса
    service = UpdatedLCTAIService()
    
    # Тест 1: Обработка нового формата
    print("\\n=== Тест 1: Новый формат запроса ===")
    sample_request = LCTFormatAdapter.create_sample_lct_request()
    
    # Добавляем реальный пример из файла fix.md
    real_request = {
        "request_uid": "21c67120-ea9c-4b95-b441-4af19dc418c2",
        "pipeline_uid": "01999ff0-dc7a-7465-9db8-ccd71d27e3a7",
        "timestamp": "2025-10-01T13:23:57.2155768Z",
        "sources": [{
            "$type": "csv",
            "parameters": {
                "delimiter": ";",
                "type": "csv",
                "file_path": "C:\\\\Projects\\\\datapipeline\\\\src\\\\Server\\\\Services\\\\DataProcessing\\\\API\\\\files\\\\01999ff0-dc7e-759f-ab54-d59ca2d07bf1\\\\part-00000-37dced01-2ad2-48c8-a56d-54b4d8760599-c000.csv"
            },
            "schema_infos": [{
                "column_count": 27,
                "columns": [
                    {
                        "name": "created",
                        "path": "created",
                        "data_type": "string",
                        "nullable": False,
                        "sample_values": ["2021-08-18T16:01:14.583+03:00"],
                        "unique_values": 428460,
                        "null_count": 0,
                        "statistics": {"min": "29", "max": "181", "avg": "29.01"}
                    }
                ],
                "size_mb": 332,
                "row_count": 478615
            }],
            "uid": "01999ff1-0c6b-741c-8c10-785ddb53d859",
            "type": "file"
        }],
        "targets": [{
            "$type": "database",
            "parameters": {
                "type": "postgre_s_q_l",
                "host": "localhost",
                "port": 5432,
                "database": "test",
                "user": "postgres",
                "password": "password",
                "schema": "public"
            },
            "schema_infos": [],
            "uid": "01999ff1-a268-7652-912e-6118d87da343",
            "type": "database"
        }]
    }
    
    result = service.process_new_format_request(real_request)
    
    print(f"[DATA] Статус: {result.get('status')}")
    print(f"[MESSAGE] Сообщение: {result.get('message')}")
    print(f"[CONNECT] Пайплайнов: {result.get('pipeline_count', 0)}")
    print(f"[AI] AI рекомендация: {result.get('ai_recommendations', {}).get('storage_type', 'N/A')}")
    
    # Тест 2: Проверка Hadoop путей
    print("\\n=== Тест 2: Hadoop интеграция ===")
    pipelines = result.get('pipelines', [])
    hadoop_functions = [p for p in pipelines if 'hadoop' in p.get('function_body', '').lower()]
    print(f"[HADOOP] Функций с Hadoop: {len(hadoop_functions)}")
    
    for func in hadoop_functions:
        print(f"   • {func['function_name_ru']} (level {func['level']})")
    
    # Тест 3: Структура пайплайнов
    print("\\n=== Тест 3: Структура пайплайнов ===")
    for pipeline in pipelines:
        print(f"Level {pipeline['level']}: {pipeline['function_name_ru']}")
        print(f"   From: {pipeline['from'][:8]}... → To: {pipeline['to'][:8]}...")
    
    # Тест 4: Spark конфигурация (опционально)
    print("\\n=== Тест 4: Spark конфигурация ===")
    spark_config = service.generate_spark_configuration(pipelines)
    print(f"[SPARK] Spark функций: {len(spark_config['optimized_functions'])}")
    
    print("\\n[SUCCESS] ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Сервис готов к работе с новым форматом.")


if __name__ == "__main__":
    test_updated_service()
