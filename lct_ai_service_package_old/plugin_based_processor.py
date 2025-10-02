"""
Обновленная архитектура для команды ЛЦТ - плагинная система
Микросервис обрабатывает: Test connection -> Get schema -> Request to AI -> Return OK
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import json
import pandas as pd
from pathlib import Path


class SourceType(Enum):
    """Типы источников данных"""
    CSV_FILE = "csv_file"
    JSON_FILE = "json_file"
    XML_FILE = "xml_file"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    CLICKHOUSE = "clickhouse"
    HDFS = "hdfs"
    S3 = "s3"


@dataclass
class ConnectionConfig:
    """Конфигурация подключения"""
    source_type: SourceType
    parameters: Dict[str, Any]


@dataclass
class SchemaField:
    """Поле схемы в формате команды ЛЦТ"""
    name: str
    data_type: str
    nullable: bool
    sample_values: List[str]
    unique_values: int
    null_count: int
    statistics: Dict[str, Any]
    filter: Optional[Dict[str, Any]] = None
    sort: Optional[str] = None


@dataclass
class SourceSchema:
    """Схема источника в формате ЛЦТ"""
    size_mb: float
    row_count: int
    fields: List[SchemaField]


class SourcePlugin(ABC):
    """Базовый класс для плагинов источников данных"""
    
    @abstractmethod
    def get_source_type(self) -> SourceType:
        """Возвращает тип источника, который обрабатывает плагин"""
        pass
    
    @abstractmethod
    async def test_connection(self, config: ConnectionConfig) -> Dict[str, Any]:
        """Тестирует подключение к источнику"""
        pass
    
    @abstractmethod
    async def get_schema(self, config: ConnectionConfig) -> SourceSchema:
        """Получает схему данных из источника"""
        pass


class CSVFilePlugin(SourcePlugin):
    """Плагин для работы с CSV файлами"""
    
    def get_source_type(self) -> SourceType:
        return SourceType.CSV_FILE
    
    async def test_connection(self, config: ConnectionConfig) -> Dict[str, Any]:
        """Проверяет доступность CSV файла"""
        try:
            filepath = config.parameters.get('filepath')
            if not filepath or not Path(filepath).exists():
                return {"status": "error", "message": f"Файл не найден: {filepath}"}
            
            # Пробуем прочитать первые строки
            delimiter = config.parameters.get('delimiter', ',')
            df_sample = pd.read_csv(filepath, delimiter=delimiter, nrows=5)
            
            return {
                "status": "success",
                "message": "CSV файл доступен",
                "preview_columns": list(df_sample.columns),
                "sample_rows": len(df_sample)
            }
            
        except Exception as e:
            return {"status": "error", "message": f"Ошибка чтения CSV: {str(e)}"}
    
    async def get_schema(self, config: ConnectionConfig) -> SourceSchema:
        """Анализирует схему CSV файла"""
        filepath = config.parameters.get('filepath')
        delimiter = config.parameters.get('delimiter', ',')
        
        # Читаем файл
        df = pd.read_csv(filepath, delimiter=delimiter)
        file_size_mb = Path(filepath).stat().st_size / (1024 * 1024)
        
        fields = []
        for column in df.columns:
            # Анализируем колонку
            series = df[column]
            
            # Определяем тип данных
            if pd.api.types.is_integer_dtype(series):
                data_type = "Integer"
                statistics = {
                    "min": str(series.min()),
                    "max": str(series.max()),
                    "avg": str(series.mean())
                }
            elif pd.api.types.is_float_dtype(series):
                data_type = "Float"
                statistics = {
                    "min": str(series.min()),
                    "max": str(series.max()),
                    "avg": str(series.mean())
                }
            else:
                data_type = "String"
                string_lengths = series.astype(str).str.len()
                statistics = {
                    "min_length": int(string_lengths.min()),
                    "max_length": int(string_lengths.max()),
                    "avg_length": int(string_lengths.mean())
                }
            
            # Собираем информацию о поле
            field = SchemaField(
                name=column,
                data_type=data_type,
                nullable=series.isnull().any(),
                sample_values=series.dropna().head(5).astype(str).tolist(),
                unique_values=series.nunique(),
                null_count=series.isnull().sum(),
                statistics=statistics
            )
            fields.append(field)
        
        return SourceSchema(
            size_mb=file_size_mb,
            row_count=len(df),
            fields=fields
        )


class JSONFilePlugin(SourcePlugin):
    """Плагин для работы с JSON файлами"""
    
    def get_source_type(self) -> SourceType:
        return SourceType.JSON_FILE
    
    async def test_connection(self, config: ConnectionConfig) -> Dict[str, Any]:
        """Проверяет доступность JSON файла"""
        try:
            filepath = config.parameters.get('filepath')
            if not filepath or not Path(filepath).exists():
                return {"status": "error", "message": f"Файл не найден: {filepath}"}
            
            # Пробуем прочитать JSON
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list) and len(data) > 0:
                sample_keys = list(data[0].keys()) if isinstance(data[0], dict) else []
            elif isinstance(data, dict):
                sample_keys = list(data.keys())
            else:
                sample_keys = []
            
            return {
                "status": "success",
                "message": "JSON файл доступен",
                "preview_keys": sample_keys[:10],
                "data_type": type(data).__name__
            }
            
        except Exception as e:
            return {"status": "error", "message": f"Ошибка чтения JSON: {str(e)}"}
    
    async def get_schema(self, config: ConnectionConfig) -> SourceSchema:
        """Анализирует схему JSON файла"""
        filepath = config.parameters.get('filepath')
        
        # Читаем JSON
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        file_size_mb = Path(filepath).stat().st_size / (1024 * 1024)
        
        # Преобразуем в DataFrame для анализа
        if isinstance(data, list):
            df = pd.json_normalize(data)
            row_count = len(data)
        else:
            df = pd.json_normalize([data])
            row_count = 1
        
        fields = []
        for column in df.columns:
            series = df[column]
            
            # Определяем тип данных (аналогично CSV)
            if pd.api.types.is_integer_dtype(series):
                data_type = "Integer"
                statistics = {"min": str(series.min()), "max": str(series.max()), "avg": str(series.mean())}
            elif pd.api.types.is_float_dtype(series):
                data_type = "Float"  
                statistics = {"min": str(series.min()), "max": str(series.max()), "avg": str(series.mean())}
            else:
                data_type = "String"
                string_lengths = series.astype(str).str.len()
                statistics = {
                    "min_length": int(string_lengths.min()),
                    "max_length": int(string_lengths.max()), 
                    "avg_length": int(string_lengths.mean())
                }
            
            field = SchemaField(
                name=column,
                data_type=data_type,
                nullable=series.isnull().any(),
                sample_values=series.dropna().head(5).astype(str).tolist(),
                unique_values=series.nunique(),
                null_count=series.isnull().sum(),
                statistics=statistics
            )
            fields.append(field)
        
        return SourceSchema(
            size_mb=file_size_mb,
            row_count=row_count,
            fields=fields
        )


class PostgreSQLPlugin(SourcePlugin):
    """Плагин для работы с PostgreSQL"""
    
    def get_source_type(self) -> SourceType:
        return SourceType.POSTGRESQL
    
    async def test_connection(self, config: ConnectionConfig) -> Dict[str, Any]:
        """Тестирует подключение к PostgreSQL"""
        try:
            import asyncpg
            
            params = config.parameters
            connection_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
            
            conn = await asyncpg.connect(connection_string)
            await conn.close()
            
            return {
                "status": "success",
                "message": "Подключение к PostgreSQL успешно",
                "database": params['database']
            }
            
        except Exception as e:
            return {"status": "error", "message": f"Ошибка подключения к PostgreSQL: {str(e)}"}
    
    async def get_schema(self, config: ConnectionConfig) -> SourceSchema:
        """Получает схему таблицы PostgreSQL"""
        import asyncpg
        
        params = config.parameters
        connection_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
        table_name = params.get('table_name')
        
        conn = await asyncpg.connect(connection_string)
        
        try:
            # Получаем информацию о колонках
            columns_info = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = $1
                ORDER BY ordinal_position
            """, table_name)
            
            # Получаем статистику по таблице
            table_stats = await conn.fetchrow(f"SELECT COUNT(*) as row_count FROM {table_name}")
            row_count = table_stats['row_count']
            
            fields = []
            for col_info in columns_info:
                # Получаем примеры значений
                sample_values = await conn.fetch(f"SELECT DISTINCT {col_info['column_name']} FROM {table_name} LIMIT 5")
                sample_values_list = [str(row[0]) for row in sample_values if row[0] is not None]
                
                # Подсчитываем null значения
                null_count_result = await conn.fetchrow(f"SELECT COUNT(*) as null_count FROM {table_name} WHERE {col_info['column_name']} IS NULL")
                null_count = null_count_result['null_count']
                
                # Подсчитываем уникальные значения
                unique_count_result = await conn.fetchrow(f"SELECT COUNT(DISTINCT {col_info['column_name']}) as unique_count FROM {table_name}")
                unique_count = unique_count_result['unique_count']
                
                # Определяем тип данных
                pg_type = col_info['data_type']
                if pg_type in ['integer', 'bigint', 'smallint']:
                    data_type = "Integer"
                elif pg_type in ['numeric', 'decimal', 'real', 'double precision']:
                    data_type = "Float"
                elif pg_type in ['timestamp', 'date', 'time']:
                    data_type = "DateTime"
                else:
                    data_type = "String"
                
                field = SchemaField(
                    name=col_info['column_name'],
                    data_type=data_type,
                    nullable=col_info['is_nullable'] == 'YES',
                    sample_values=sample_values_list,
                    unique_values=unique_count,
                    null_count=null_count,
                    statistics={}  # Можно добавить min/max/avg для числовых полей
                )
                fields.append(field)
            
            # Примерно оцениваем размер таблицы
            size_result = await conn.fetchrow(f"SELECT pg_total_relation_size('{table_name}') as size_bytes")
            size_mb = size_result['size_bytes'] / (1024 * 1024) if size_result['size_bytes'] else 0
            
            return SourceSchema(
                size_mb=size_mb,
                row_count=row_count,
                fields=fields
            )
            
        finally:
            await conn.close()


class PluginBasedDataProcessor:
    """Обобщенный микросервис для обработки данных с плагинами"""
    
    def __init__(self):
        self.plugins: Dict[SourceType, SourcePlugin] = {}
        self._register_default_plugins()
    
    def _register_default_plugins(self):
        """Регистрирует стандартные плагины"""
        plugins = [
            CSVFilePlugin(),
            JSONFilePlugin(),
            PostgreSQLPlugin(),
        ]
        
        for plugin in plugins:
            self.register_plugin(plugin)
    
    def register_plugin(self, plugin: SourcePlugin):
        """Регистрирует плагин для обработки определенного типа источника"""
        self.plugins[plugin.get_source_type()] = plugin
    
    async def test_connection(self, config: ConnectionConfig) -> Dict[str, Any]:
        """Test connection - обобщенный метод"""
        if config.source_type not in self.plugins:
            return {
                "status": "error",
                "message": f"Плагин для типа {config.source_type.value} не найден"
            }
        
        plugin = self.plugins[config.source_type]
        return await plugin.test_connection(config)
    
    async def get_schema(self, config: ConnectionConfig) -> SourceSchema:
        """Get schema - обобщенный метод с делегированием в плагин"""
        if config.source_type not in self.plugins:
            raise ValueError(f"Плагин для типа {config.source_type.value} не найден")
        
        plugin = self.plugins[config.source_type]
        return await plugin.get_schema(config)
    
    async def process_source(self, config: ConnectionConfig) -> Dict[str, Any]:
        """Полный цикл: Test connection -> Get schema -> Return OK"""
        try:
            # 1. Test connection
            connection_result = await self.test_connection(config)
            if connection_result["status"] != "success":
                return connection_result
            
            # 2. Get schema
            schema = await self.get_schema(config)
            
            # 3. Return OK with schema
            return {
                "status": "success",
                "connection_test": connection_result,
                "schema": {
                    "size_mb": schema.size_mb,
                    "row_count": schema.row_count,
                    "fields": [
                        {
                            "name": field.name,
                            "data_type": field.data_type,
                            "nullable": field.nullable,
                            "sample_values": field.sample_values,
                            "unique_values": field.unique_values,
                            "null_count": field.null_count,
                            "statistics": field.statistics
                        }
                        for field in schema.fields
                    ]
                }
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Ошибка обработки источника: {str(e)}"
            }

# Пример использования
async def example_usage():
    processor = PluginBasedDataProcessor()
    
    # CSV файл
    csv_config = ConnectionConfig(
        source_type=SourceType.CSV_FILE,
        parameters={
            "filepath": "C:/data/sample.csv",
            "delimiter": ","
        }
    )
    
    result = await processor.process_source(csv_config)
    print("CSV результат:", json.dumps(result, indent=2, ensure_ascii=False))
    
    # PostgreSQL таблица
    pg_config = ConnectionConfig(
        source_type=SourceType.POSTGRESQL,
        parameters={
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "postgres",
            "password": "password",
            "table_name": "users"
        }
    )
    
    result = await processor.process_source(pg_config)
    print("PostgreSQL результат:", json.dumps(result, indent=2, ensure_ascii=False))