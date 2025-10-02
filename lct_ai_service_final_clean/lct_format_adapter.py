"""
LCT Format Adapter для обработки новых форматов запросов
Исправленная версия с поддержкой всех требований fix.md
"""
import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class LCTFormatAdapter:
    """Адаптер для работы с новыми форматами LCT"""
    
    def __init__(self):
        logger.info("LCTFormatAdapter инициализирован")
    
    def parse_new_format_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Парсинг запроса в новом формате fix.md"""
        try:
            logger.info("Парсинг запроса в новом формате LCT (fix.md)")
            
            # Извлечение основных параметров по новому формату
            parsed = {
                "request_uid": request_data.get("request_uid", "unknown"),
                "pipeline_uid": request_data.get("pipeline_uid", "unknown"),
                "timestamp": request_data.get("timestamp", ""),
                "sources": self._parse_sources(request_data.get("sources", [])),
                "targets": self._parse_targets(request_data.get("targets", [])),
                "raw_sources": request_data.get("sources", []),
                "raw_targets": request_data.get("targets", [])
            }
            
            # Валидация обязательных полей
            if not parsed["sources"]:
                raise ValueError("Отсутствуют источники данных в запросе")
            
            if not parsed["targets"]:
                raise ValueError("Отсутствуют целевые системы в запросе")
            
            logger.info(f"Запрос успешно распарсен (новый формат): {parsed['request_uid']}")
            return parsed
            
        except Exception as e:
            logger.error(f"Ошибка парсинга запроса: {e}")
            raise
    
    def _parse_sources(self, sources: List[Dict]) -> List[Dict]:
        """Парсинг источников в новом формате fix.md"""
        parsed_sources = []
        for source in sources:
            # Новый формат fix.md поддерживает поля: name, format, estimated_size, source_type, update_frequency, data_characteristics
            parsed_source = {
                "uid": source.get("name", source.get("uid", "unknown")),  # Используем name из нового формата
                "type": self._map_source_type(source.get("source_type", "file")),
                "$type": self._map_format_type(source.get("format", "csv")),
                "parameters": source.get("parameters", {}),
                "schema_infos": source.get("schema_infos", []),
                "hadoop_file_path": self._extract_hadoop_path(source.get("parameters", {})),
                # Сохраняем оригинальные данные для анализатора
                "original_data": source  # Важно для передачи в анализатор
            }
            parsed_sources.append(parsed_source)
        return parsed_sources
    
    def _parse_targets(self, targets: List[Dict]) -> List[Dict]:
        """Парсинг целевых систем в новом формате"""
        parsed_targets = []
        for target in targets:
            parsed_target = {
                "uid": target.get("uid", "unknown"),
                "type": target.get("type", "database"),
                "$type": target.get("$type", "database"),
                "parameters": target.get("parameters", {}),
                "schema_infos": target.get("schema_infos", [])
            }
            parsed_targets.append(parsed_target)
        return parsed_targets
    
    def _map_source_type(self, source_type: str) -> str:
        """Маппинг типа источника для совместимости"""
        mapping = {
            "database": "database",
            "streaming": "stream", 
            "files": "file",
            "file": "file"
        }
        return mapping.get(source_type, "file")
    
    def _map_format_type(self, format_type: str) -> str:
        """Маппинг формата данных для совместимости"""
        mapping = {
            "json": "json",
            "csv": "csv",
            "postgresql": "database",
            "mysql": "database",
            "parquet": "parquet",
            "xml": "xml"
        }
        return mapping.get(format_type, "csv")

    def _map_source_type(self, source_type: str) -> str:
        """Маппинг типа источника для совместимости"""
        mapping = {
            "database": "database",
            "streaming": "stream", 
            "files": "file",
            "file": "file"
        }
        return mapping.get(source_type, "file")
    
    def _map_format_type(self, format_type: str) -> str:
        """Маппинг формата данных для совместимости"""
        mapping = {
            "json": "json",
            "csv": "csv",
            "postgresql": "database",
            "mysql": "database",
            "parquet": "parquet",
            "xml": "xml"
        }
        return mapping.get(format_type, "csv")

    def _extract_hadoop_path(self, parameters: Dict) -> str:
        """Извлечение Hadoop пути из file_path"""
        file_path = parameters.get("file_path", "")
        if file_path and "files" in file_path:
            # Конвертируем Windows путь в Hadoop путь
            hadoop_path = file_path.replace("C:\\Projects\\datapipeline\\src\\Server\\Services\\DataProcessing\\API\\files\\", "/hadoop/data/")
            hadoop_path = hadoop_path.replace("\\", "/")
            return hadoop_path
        return file_path
    
    def convert_to_legacy_format(self, parsed_request: Dict[str, Any]) -> Dict[str, Any]:
        """Конвертация в устаревший формат для совместимости"""
        try:
            legacy_format = {
                "task_id": parsed_request["request_uid"],
                "pipeline_id": parsed_request["pipeline_uid"],
                "source_type": "hadoop",
                "target_type": "database",
                "data_sources": self._convert_sources(parsed_request["sources"]),
                "data_targets": self._convert_targets(parsed_request["targets"]),
                "processing_options": parsed_request.get("settings", {})
            }
            
            logger.info("Конвертация в legacy формат завершена")
            return legacy_format
            
        except Exception as e:
            logger.error(f"Ошибка конвертации в legacy формат: {e}")
            raise
    
    def convert_to_hadoop_config(self, parsed_request: Dict[str, Any]) -> Dict[str, Any]:
        """Конвертация в конфигурацию Hadoop"""
        try:
            sources = parsed_request["sources"]
            targets = parsed_request["targets"]
            
            # Анализ размера данных для определения стратегии выполнения
            total_size_mb = 0
            total_rows = 0
            
            for source in sources:
                size_info = source.get("size", {})
                total_size_mb += size_info.get("mb", 0)
                total_rows += size_info.get("rows", 0)
            
            # Определение стратегии выполнения
            use_spark_distribution = total_size_mb > 500 or total_rows > 1000000
            
            hadoop_config = {
                "cluster_mode": "distributed" if use_spark_distribution else "local",
                "spark_enabled": use_spark_distribution,
                "data_size_mb": total_size_mb,
                "estimated_rows": total_rows,
                "source_paths": [s.get("path", "") for s in sources],
                "target_connections": [t.get("connection", {}) for t in targets],
                "optimization_level": "high" if use_spark_distribution else "standard",
                "parallelism_factor": min(8, max(1, total_size_mb // 100)) if use_spark_distribution else 1
            }
            
            logger.info(f"Hadoop конфигурация: Spark={'включен' if use_spark_distribution else 'отключен'}, размер={total_size_mb}MB")
            return hadoop_config
            
        except Exception as e:
            logger.error(f"Ошибка создания Hadoop конфигурации: {e}")
            raise
    
    def _convert_sources(self, sources: List[Dict]) -> List[Dict]:
        """Конвертация источников данных"""
        converted = []
        for source in sources:
            converted_source = {
                "id": source.get("id", "unknown"),
                "type": source.get("type", "csv"),
                "location": source.get("path", ""),
                "format": source.get("format", "csv"),
                "schema": source.get("schema", {}),
                "size_estimate": source.get("size", {})
            }
            converted.append(converted_source)
        
        return converted
    
    def _convert_targets(self, targets: List[Dict]) -> List[Dict]:
        """Конвертация целевых систем"""
        converted = []
        for target in targets:
            converted_target = {
                "id": target.get("id", "unknown"),
                "type": target.get("type", "database"),
                "connection": target.get("connection", {}),
                "table_name": target.get("table", "migrated_data"),
                "write_mode": target.get("mode", "overwrite")
            }
            converted.append(converted_target)
        
        return converted
    
    def format_response(self, pipeline_result: Dict[str, Any], 
                       request_uid: str, pipeline_uid: str) -> Dict[str, Any]:
        """Форматирование ответа в требуемом формате"""
        try:
            # Базовая структура ответа
            response = {
                "uid": request_uid,
                "pipelineUID": pipeline_uid,
                "status": "success",
                "created_at": pipeline_result.get("created_at", ""),
                "template": pipeline_result.get("template", ""),
                "pipelines": pipeline_result.get("pipelines", []),
                "migration_report": pipeline_result.get("migration_report", "")
            }
            
            # Добавление метаданных
            if "metadata" in pipeline_result:
                response["metadata"] = pipeline_result["metadata"]
            
            # Добавление ошибок если есть
            if "errors" in pipeline_result:
                response["errors"] = pipeline_result["errors"]
                response["status"] = "error"
            
            logger.info(f"Ответ сформатирован для запроса {request_uid}")
            return response
            
        except Exception as e:
            logger.error(f"Ошибка форматирования ответа: {e}")
            raise
    
    def validate_request_format(self, request_data: Dict[str, Any]) -> bool:
        """Валидация формата запроса для нового формата fix.md"""
        try:
            required_fields = ["request_uid", "pipeline_uid", "sources", "targets"]
            
            for field in required_fields:
                if field not in request_data:
                    logger.error(f"Отсутствует обязательное поле: {field}")
                    return False
            
            # Проверка источников
            sources = request_data.get("sources", [])
            if not isinstance(sources, list) or len(sources) == 0:
                logger.error("Источники данных должны быть непустым списком")
                return False
            
            # Проверка целей
            targets = request_data.get("targets", [])
            if not isinstance(targets, list) or len(targets) == 0:
                logger.error("Целевые системы должны быть непустым списком")
                return False
            
            logger.info("Валидация формата запроса пройдена успешно (новый формат)")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка валидации формата запроса: {e}")
            return False
    
    def extract_pipeline_metadata(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Извлечение метаданных пайплайна"""
        try:
            metadata = {
                "request_timestamp": request_data.get("timestamp", ""),
                "user_id": request_data.get("user_id", "system"),
                "environment": request_data.get("environment", "production"),
                "priority": request_data.get("priority", "normal"),
                "tags": request_data.get("tags", []),
                "source_count": len(request_data.get("sources", [])),
                "target_count": len(request_data.get("targets", [])),
                "pipeline_type": "hadoop",  # По умолчанию hadoop
                "estimated_complexity": self._calculate_complexity(request_data)
            }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Ошибка извлечения метаданных: {e}")
            return {}
    
    def _calculate_complexity(self, request_data: Dict[str, Any]) -> str:
        """Расчет сложности пайплайна"""
        try:
            sources = request_data.get("sources", [])
            targets = request_data.get("targets", [])
            
            complexity_score = 0
            
            # Базовая сложность от количества источников и целей
            complexity_score += len(sources) * 2
            complexity_score += len(targets) * 3
            
            # Сложность от размера данных
            for source in sources:
                size_info = source.get("size", {})
                size_mb = size_info.get("mb", 0)
                if size_mb > 1000:
                    complexity_score += 5
                elif size_mb > 100:
                    complexity_score += 3
                elif size_mb > 10:
                    complexity_score += 1
            
            # Определение уровня сложности
            if complexity_score <= 5:
                return "low"
            elif complexity_score <= 15:
                return "medium"
            else:
                return "high"
                
        except Exception as e:
            logger.error(f"Ошибка расчета сложности: {e}")
            return "unknown"
