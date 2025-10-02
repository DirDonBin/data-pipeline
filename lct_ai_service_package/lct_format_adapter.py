"""
Адаптер форматов данных для интеграции с командой ЛЦТ
Конвертирует между нашими внутренними форматами и их API форматом
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import uuid
from datetime import datetime


class FieldDataType(Enum):
    """Типы данных полей"""
    INTEGER = "Integer"
    STRING = "String"
    FLOAT = "Float"
    BOOLEAN = "Boolean"
    DATE = "Date"
    DATETIME = "DateTime"
    DECIMAL = "Decimal"


class FilterOperation(Enum):
    """Операции фильтрации"""
    EQ = "eq"  # равно
    NE = "ne"  # не равно
    GT = "gt"  # больше
    GE = "ge"  # больше или равно
    LT = "lt"  # меньше
    LE = "le"  # меньше или равно
    IN = "in"  # в списке
    NOT_IN = "not_in"  # не в списке
    LIKE = "like"  # похоже (для строк)


@dataclass
class LCTFieldFilter:
    """Фильтр поля в формате команды"""
    op: str
    value: Any


@dataclass
class LCTFieldStatistics:
    """Статистика поля в формате команды"""
    min: Optional[str] = None
    max: Optional[str] = None
    avg: Optional[str] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[int] = None


@dataclass
class LCTField:
    """Поле в формате команды"""
    name: str
    data_type: str
    nullable: bool
    sample_values: List[str]
    unique_values: int
    null_count: int
    statistics: LCTFieldStatistics
    filter: Optional[LCTFieldFilter] = None
    sort: Optional[str] = None  # "ASC" или "DESC"
    is_primary: Optional[bool] = None
    fields: Optional[List['LCTField']] = None  # Для вложенных структур


@dataclass
class LCTSchemaInfo:
    """Информация о схеме в формате команды"""
    size_mb: float
    row_count: int
    fields: List[LCTField]


@dataclass
class LCTFileSource:
    """Файловый источник в формате команды"""
    type: str = "File"
    parameters: Dict[str, Any] = None
    schema_infos: List[LCTSchemaInfo] = None


@dataclass
class LCTDatabaseTarget:
    """Целевая база данных в формате команды"""
    type: str = "Database"
    parameters: Dict[str, Any] = None
    schema_infos: List[LCTSchemaInfo] = None


@dataclass
class LCTPipelineRequest:
    """Запрос на создание пайплайна в формате команды"""
    request_uid: str
    pipeline_uid: str
    timestamp: str
    sources: List[LCTFileSource]
    targets: List[LCTDatabaseTarget]


@dataclass
class LCTPipelineResponse:
    """Ответ на запрос создания пайплайна"""
    request_uid: str
    pipeline_uid: str
    timestamp: str
    status: str
    storage_recommendation: Optional[Dict[str, Any]] = None
    artifacts: Optional[Dict[str, Any]] = None
    dag_config: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


class LCTFormatAdapter:
    """Адаптер для конвертации между форматами"""
    
    @staticmethod
    def convert_our_schema_to_lct_field(column_info: Dict[str, Any]) -> LCTField:
        """Конвертация нашего формата колонки в формат команды"""
        
        # Определяем тип данных
        our_type = column_info.get('data_type', 'string').lower()
        lct_type_mapping = {
            'int64': 'Integer',
            'float64': 'Float', 
            'object': 'String',
            'bool': 'Boolean',
            'datetime64[ns]': 'DateTime',
            'date': 'Date'
        }
        lct_type = lct_type_mapping.get(our_type, 'String')
        
        # Статистики
        stats = column_info.get('statistics', {})
        if lct_type in ['Integer', 'Float', 'Decimal']:
            statistics = LCTFieldStatistics(
                min=str(stats.get('min', '')),
                max=str(stats.get('max', '')),
                avg=str(stats.get('mean', ''))
            )
        else:
            statistics = LCTFieldStatistics(
                min_length=stats.get('min_length'),
                max_length=stats.get('max_length'),
                avg_length=stats.get('avg_length')
            )
        
        return LCTField(
            name=column_info['name'],
            data_type=lct_type,
            nullable=column_info.get('nullable', True),
            sample_values=column_info.get('sample_values', [])[:5],
            unique_values=column_info.get('unique_count', 0),
            null_count=column_info.get('null_count', 0),
            statistics=statistics
        )
    
    @staticmethod
    def convert_our_file_to_lct_source(file_info: Dict[str, Any]) -> LCTFileSource:
        """Конвертация информации о нашем файле в формат команды"""
        
        # Параметры файла
        file_format = file_info.get('format', 'csv').upper()
        parameters = {
            "type": file_format,
            "filepath": file_info.get('path', ''),
        }
        
        if file_format == 'CSV':
            parameters["delimiter"] = file_info.get('delimiter', ',')
        
        # Схема
        fields = []
        for column in file_info.get('columns', []):
            lct_field = LCTFormatAdapter.convert_our_schema_to_lct_field(column)
            fields.append(lct_field)
        
        schema_info = LCTSchemaInfo(
            size_mb=file_info.get('size_mb', 0),
            row_count=file_info.get('row_count', 0),
            fields=fields
        )
        
        return LCTFileSource(
            type="File",
            parameters=parameters,
            schema_infos=[schema_info]
        )
    
    @staticmethod
    def convert_lct_request_to_our_format(lct_request: Dict[str, Any]) -> Dict[str, Any]:
        """Конвертация запроса команды в наш внутренний формат"""
        
        our_sources = []
        for source in lct_request.get('sources', []):
            source_type = source.get('type', '')
            if source_type == 'File':
                schema_info = source.get('schema-infos', [{}])[0] if source.get('schema-infos') else {}
                
                our_columns = []
                for field in schema_info.get('fields', []):
                    our_column = {
                        'name': field.get('name', ''),
                        'data_type': field.get('data_type', 'varchar').lower(),
                        'nullable': field.get('nullable', True),
                        'sample_values': field.get('sample_values', []),
                        'unique_count': field.get('unique_values', 0),
                        'null_count': field.get('null_count', 0),
                        'statistics': field.get('statistics', {})
                    }
                    our_columns.append(our_column)
                
                our_source = {
                    'uid': f"source_{len(our_sources)}",
                    'type': 'file',
                    'format': source.get('parameters', {}).get('type', 'csv').lower(),
                    'path': source.get('parameters', {}).get('filepath', ''),
                    'size_mb': schema_info.get('size_mb', 0),
                    'row_count': schema_info.get('row_count', 0),
                    'columns': our_columns
                }
                our_sources.append(our_source)
        
        our_targets = []
        for target in lct_request.get('targets', []):
            target_type = target.get('type', '')
            if target_type == 'Database':
                our_target = {
                    'uid': f"target_{len(our_targets)}",
                    'type': 'database',
                    'database_type': target.get('parameters', {}).get('type', 'postgresql').lower(),
                    'connection': target.get('parameters', {}),
                    'tables': []
                }
                
                # Добавляем информацию о таблицах если есть
                if 'schema-infos' in target and len(target['schema-infos']) > 0:
                    schema_info = target['schema-infos'][0]
                    for table in schema_info.get('tables', []):
                        our_target['tables'].append({
                            'name': table['name'],
                            'columns': table['columns']
                        })
                
                our_targets.append(our_target)
        
        return {
            'request_uid': lct_request['request_uid'],
            'pipeline_uid': lct_request['pipeline_uid'],
            'sources': our_sources,
            'targets': our_targets
        }
    
    @staticmethod
    def convert_our_response_to_lct_format(our_response: Dict[str, Any]) -> Dict[str, Any]:
        """Конвертация нашего ответа в формат команды согласно fix.md"""
        
        lct_response = {
            "request_uid": our_response['request_uid'],
            "pipeline_uid": our_response['pipeline_uid'],
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
        # Рекомендация по хранилищу
        if 'storage_recommendation' in our_response:
            storage = our_response['storage_recommendation']
            lct_response["storage_recommendation"] = {
                "type": storage.get('type', 'PostgreSQL'),
                "confidence": storage.get('confidence', 0.8),
                "reasoning": storage.get('reasoning', 'Автоматический выбор')
            }
        
        # НОВЫЙ ФОРМАТ СОГЛАСНО fix.md - template и pipelines вместо artifacts
        template_content = ""
        pipelines_list = []
        
        # Template - код DAG файла БЕЗ функций
        if 'dag_config' in our_response and 'template' in our_response['dag_config']:
            template_content = our_response['dag_config']['template']
        elif 'artifacts' in our_response and 'dag_content' in our_response['artifacts']:
            # Fallback: берем template из dag_content в artifacts
            template_content = our_response['artifacts']['dag_content']
            
        if isinstance(template_content, dict):
            template_content = str(template_content)
        lct_response["template"] = template_content
            
        # Pipelines - список пайплайнов с детальной информацией
        if 'artifacts' in our_response and 'pipelines' in our_response['artifacts']:
            pipelines_list = our_response['artifacts']['pipelines']
        lct_response["pipelines"] = pipelines_list
        
        # Для обратной совместимости также сохраняем старые поля
        if 'artifacts' in our_response:
            artifacts = our_response['artifacts']
            lct_response["artifacts"] = {
                "ddl_script": artifacts.get('ddl_script', ''),
                "dag_content": artifacts.get('dag_content', ''),
                "migration_report": artifacts.get('migration_report', ''),
                "pipelines": artifacts.get('pipelines', [])
            }
            # Также добавляем dag_content на верхний уровень для совместимости
            lct_response["dag_content"] = artifacts.get('dag_content', '')
        
        # Конфигурация DAG
        if 'dag_config' in our_response:
            lct_response["dag_config"] = our_response['dag_config']
        else:
            # Конфигурация по умолчанию
            lct_response["dag_config"] = {
                "dag_id": f"pipeline_{our_response['pipeline_uid']}",
                "schedule_interval": "@daily",
                "start_date": datetime.now().isoformat(),
                "catchup": False
            }
        
        return lct_response
    
    @staticmethod
    def create_sample_lct_request() -> Dict[str, Any]:
        """Создание примера запроса в формате команды для тестирования"""
        
        return {
            "request_uid": str(uuid.uuid4()),
            "pipeline_uid": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "sources": [
                {
                    "type": "File",
                    "parameters": {
                        "type": "CSV",
                        "filepath": "C:/data/sample.csv",
                        "delimiter": ","
                    },
                    "schema-infos": [
                        {
                            "size_mb": 100.5,
                            "row_count": 50000,
                            "fields": [
                                {
                                    "name": "id",
                                    "data_type": "Integer",
                                    "filter": None,
                                    "sort": None,
                                    "nullable": False,
                                    "sample_values": ["1", "2", "3", "4", "5"],
                                    "unique_values": 50000,
                                    "null_count": 0,
                                    "statistics": {
                                        "min": "1",
                                        "max": "50000",
                                        "avg": "25000.5"
                                    }
                                },
                                {
                                    "name": "name",
                                    "data_type": "String", 
                                    "filter": None,
                                    "sort": None,
                                    "nullable": True,
                                    "sample_values": ["Иван", "Мария", "Петр", "Анна", "Сергей"],
                                    "unique_values": 45000,
                                    "null_count": 100,
                                    "statistics": {
                                        "min_length": 3,
                                        "max_length": 20,
                                        "avg_length": 8
                                    }
                                }
                            ]
                        }
                    ]
                }
            ],
            "targets": [
                {
                    "type": "Database",
                    "parameters": {
                        "type": "PostgreSQL",
                        "host": "localhost",
                        "port": 5432,
                        "database": "lct_db",
                        "user": "postgres",
                        "password": "postgres"
                    },
                    "schema-infos": []
                }
            ]
        }