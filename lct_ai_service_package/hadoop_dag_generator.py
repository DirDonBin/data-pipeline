"""
Новый DAG генератор с поддержкой Hadoop путей и разделения на пайплайны
Соответствует требованиям команды ЛЦТ по структуре pipelines
"""

from typing import Dict, List, Any, Optional, Tuple
import uuid
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)


class HadoopDAGGenerator:
    """Генератор DAG с поддержкой Hadoop и разделения на пайплайны"""
    
    def __init__(self, enable_spark_distribution: bool = False):
        self.enable_spark_distribution = enable_spark_distribution
        self.pipeline_dependencies = {
            "create_target_schema": [],
            "extract_data": ["create_target_schema"],
            "transform_data": ["extract_data"],
            "load_data": ["transform_data"],
            "validate_data": ["load_data"],
            "create_indexes": ["validate_data"],
            "generate_report": ["create_indexes"]
        }
        
        # Функции которые могут быть распределены на Spark
        self.spark_distributable_functions = ["extract_data", "transform_data"]
        
        # Пороги для автоматического включения Spark
        self.spark_auto_threshold_mb = 500  # Больше 500 MB
        self.spark_auto_threshold_rows = 1000000  # Больше 1 млн строк
    
    def _should_use_spark_for_data(self, sources: list) -> bool:
        """Определяет, нужно ли использовать Spark для данного объема данных"""
        total_size_mb = 0
        total_rows = 0
        
        for source in sources:
            if isinstance(source, dict):
                schema_infos = source.get("schema_infos", [])
                for schema_info in schema_infos:
                    if isinstance(schema_info, dict):
                        total_size_mb += schema_info.get("size_mb", 0)
                        total_rows += schema_info.get("row_count", 0)
        
        should_use_spark = (
            total_size_mb > self.spark_auto_threshold_mb or 
            total_rows > self.spark_auto_threshold_rows
        )
        
        if should_use_spark:
            logger.info(f"[SPARK_AUTO] Автоматическое включение Spark: {total_size_mb:.1f}MB, {total_rows:,} строк")
        
        return should_use_spark
    
    def _is_function_spark_distributed(self, function_name: str, sources: list = None) -> bool:
        """Определяет, должна ли функция выполняться распределенно на Spark"""
        if not self.enable_spark_distribution:
            return False
            
        if function_name not in self.spark_distributable_functions:
            return False
            
        # Если есть информация об источниках, проверяем размер данных
        if sources and not self._should_use_spark_for_data(sources):
            return False
            
        return True
    
    def _get_level_for_function(self, function_name: str) -> int:
        """Определяет уровень функции в DAG"""
        levels = {
            "create_target_schema": 0,
            "extract_data": 1,
            "transform_data": 2,
            "load_data": 3,
            "validate_data": 4,
            "create_indexes": 5,
            "generate_report": 6
        }
        return levels.get(function_name, 0)
    
    def _generate_hadoop_extract_function(self, source: Dict[str, Any]) -> str:
        """Генерирует функцию извлечения данных из Hadoop"""
        
        hadoop_path = source.get("parameters", {}).get("file_path", "")
        source_type = source.get("parameters", {}).get("type", "csv")
        delimiter = source.get("parameters", {}).get("delimiter", ",")
        
        if source_type == "csv":
            return f'''
def extract_data(**context):
    \"\"\"Извлечение данных из Hadoop CSV файла\"\"\"
    from pyspark.sql import SparkSession
    import pandas as pd
    
    try:
        # Инициализация Spark сессии для работы с Hadoop
        spark = SparkSession.builder \\
            .appName("LCT_Data_Extract") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
            .getOrCreate()
        
        # Чтение данных из Hadoop
        hadoop_path = "{hadoop_path}"
        print(f"Извлечение данных из Hadoop: {{hadoop_path}}")
        
        # Чтение CSV с настройками
        df = spark.read \\
            .option("header", "true") \\
            .option("delimiter", "{delimiter}") \\
            .option("inferSchema", "true") \\
            .option("multiline", "true") \\
            .option("escape", "\\"") \\
            .csv(hadoop_path)
        
        # Базовая валидация
        row_count = df.count()
        print(f"Извлечено записей: {{row_count}}")
        
        if row_count == 0:
            raise ValueError("Файл пуст или не содержит данных")
        
        # Конвертация в Pandas для дальнейшей обработки (если нужно)
        pandas_df = df.toPandas()
        
        # Сохранение в временное хранилище для следующих задач
        temp_path = "/tmp/extracted_data.parquet"
        df.write.mode("overwrite").parquet(temp_path)
        
        # Передача метаданных в XCom
        context['task_instance'].xcom_push(
            key='extracted_data_path', 
            value=temp_path
        )
        context['task_instance'].xcom_push(
            key='row_count', 
            value=row_count
        )
        context['task_instance'].xcom_push(
            key='schema', 
            value=df.schema.json()
        )
        
        print("[SUCCESS] Данные успешно извлечены из Hadoop")
        return temp_path
        
    except Exception as e:
        print(f"[ERROR] Ошибка извлечения данных: {{e}}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
'''
        
        elif source_type == "json":
            return f'''
def extract_data(**context):
    \"\"\"Извлечение данных из Hadoop JSON файла\"\"\"
    from pyspark.sql import SparkSession
    
    try:
        # Инициализация Spark для JSON
        spark = SparkSession.builder \\
            .appName("LCT_JSON_Extract") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .getOrCreate()
        
        # Чтение JSON из Hadoop
        hadoop_path = "{hadoop_path}"
        print(f"Извлечение JSON данных из Hadoop: {{hadoop_path}}")
        
        df = spark.read \\
            .option("multiline", "true") \\
            .option("mode", "PERMISSIVE") \\
            .option("columnNameOfCorruptRecord", "_corrupt_record") \\
            .json(hadoop_path)
        
        row_count = df.count()
        print(f"Извлечено JSON записей: {{row_count}}")
        
        # Сохранение в Parquet для эффективности
        temp_path = "/tmp/extracted_json_data.parquet"
        df.write.mode("overwrite").parquet(temp_path)
        
        # Метаданные в XCom
        context['task_instance'].xcom_push(key='extracted_data_path', value=temp_path)
        context['task_instance'].xcom_push(key='row_count', value=row_count)
        
        print("[SUCCESS] JSON данные успешно извлечены из Hadoop")
        return temp_path
        
    except Exception as e:
        print(f"[ERROR] Ошибка извлечения JSON: {{e}}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
'''
        
        else:
            # Fallback для других типов
            return f'''
def extract_data(**context):
    \"\"\"Универсальное извлечение данных из Hadoop\"\"\"
    import subprocess
    
    hadoop_path = "{hadoop_path}"
    print(f"Извлечение данных из Hadoop: {{hadoop_path}}")
    
    # Базовая проверка существования файла в Hadoop
    result = subprocess.run([
        "hdfs", "dfs", "-test", "-e", hadoop_path
    ], capture_output=True)
    
    if result.returncode != 0:
        raise FileNotFoundError(f"Файл не найден в Hadoop: {{hadoop_path}}")
    
    print("[SUCCESS] Файл найден в Hadoop")
    return hadoop_path
'''
    
    def _generate_transform_function(self, sources: List[Dict[str, Any]]) -> str:
        """Генерирует функцию трансформации данных"""
        return '''
def transform_data(**context):
    \"\"\"Трансформация данных с поддержкой Spark\"\"\"
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    
    try:
        # Инициализация Spark для трансформации
        spark = SparkSession.builder \\
            .appName("LCT_Data_Transform") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
            .getOrCreate()
        
        # Получение пути к данным из предыдущей задачи
        extracted_path = context['task_instance'].xcom_pull(
            task_ids='extract_data', 
            key='extracted_data_path'
        )
        
        print(f"Трансформация данных из: {extracted_path}")
        
        # Загрузка данных
        df = spark.read.parquet(extracted_path)
        
        # AI-генерированные трансформации
        print("Применение AI трансформаций...")
        
        # 1. Очистка данных
        df_clean = df.dropna(how='all')  # Удаление полностью пустых строк
        
        # 2. Стандартизация строковых полей
        string_columns = [f.name for f in df_clean.schema.fields if f.dataType == StringType()]
        for col_name in string_columns:
            df_clean = df_clean.withColumn(
                col_name, 
                trim(upper(col(col_name)))
            )
        
        # 3. Добавление метаданных
        df_transformed = df_clean \\
            .withColumn("processed_at", current_timestamp()) \\
            .withColumn("processing_version", lit("1.0")) \\
            .withColumn("data_source", lit("hadoop_pipeline"))
        
        # 4. Валидация после трансформации
        transformed_count = df_transformed.count()
        original_count = context['task_instance'].xcom_pull(
            task_ids='extract_data', 
            key='row_count'
        )
        
        data_quality_ratio = transformed_count / original_count if original_count > 0 else 0
        print(f"Качество данных: {data_quality_ratio:.2%} ({transformed_count}/{original_count})")
        
        if data_quality_ratio < 0.8:
            print("[WARNING] Предупреждение: низкое качество данных после трансформации")
        
        # Сохранение трансформированных данных
        transformed_path = "/tmp/transformed_data.parquet"
        df_transformed.write.mode("overwrite").parquet(transformed_path)
        
        # Передача метаданных
        context['task_instance'].xcom_push(key='transformed_data_path', value=transformed_path)
        context['task_instance'].xcom_push(key='transformed_count', value=transformed_count)
        context['task_instance'].xcom_push(key='data_quality_ratio', value=data_quality_ratio)
        
        print("[SUCCESS] Трансформация данных завершена")
        return transformed_path
        
    except Exception as e:
        print(f"[ERROR] Ошибка трансформации: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
'''
    
    def _generate_load_function(self, target: Dict[str, Any]) -> str:
        """Генерирует функцию загрузки данных в целевую БД"""
        
        db_params = target.get("parameters", {})
        
        return f'''
def load_data(**context):
    \"\"\"Загрузка данных в целевую базу данных\"\"\"
    from pyspark.sql import SparkSession
    import psycopg2
    from sqlalchemy import create_engine
    
    try:
        # Параметры подключения к БД
        db_config = {{
            "host": "{db_params.get('host', 'localhost')}",
            "port": {db_params.get('port', 5432)},
            "database": "{db_params.get('database', 'test')}",
            "user": "{db_params.get('user', 'postgres')}",
            "password": "{db_params.get('password', 'password')}",
            "schema": "{db_params.get('schema', 'public')}"
        }}
        
        # Получение пути к трансформированным данным
        transformed_path = context['task_instance'].xcom_pull(
            task_ids='transform_data', 
            key='transformed_data_path'
        )
        
        # Инициализация Spark с поддержкой PostgreSQL
        spark = SparkSession.builder \\
            .appName("LCT_Data_Load") \\
            .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.0.jar") \\
            .getOrCreate()
        
        # Загрузка трансформированных данных
        df = spark.read.parquet(transformed_path)
        
        # Подключение к PostgreSQL
        jdbc_url = f"jdbc:postgresql://{{db_config['host']}}:{{db_config['port']}}/{{db_config['database']}}"
        
        # Запись данных в БД
        df.write \\
            .format("jdbc") \\
            .option("url", jdbc_url) \\
            .option("dbtable", f"{{db_config['schema']}}.migrated_data") \\
            .option("user", db_config['user']) \\
            .option("password", db_config['password']) \\
            .option("driver", "org.postgresql.Driver") \\
            .mode("overwrite") \\
            .save()
        
        loaded_count = df.count()
        
        # Метаданные загрузки
        context['task_instance'].xcom_push(key='loaded_count', value=loaded_count)
        context['task_instance'].xcom_push(key='target_table', value=f"{{db_config['schema']}}.migrated_data")
        
        print(f"[SUCCESS] Загружено записей: {{loaded_count}}")
        return loaded_count
        
    except Exception as e:
        print(f"[ERROR] Ошибка загрузки: {{e}}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
'''
    
    def _generate_spark_extract_function(self, source: Dict[str, Any]) -> str:
        """Генерирует Spark-распределенную функцию извлечения данных"""
        
        hadoop_path = source.get("parameters", {}).get("file_path", "")
        source_type = source.get("parameters", {}).get("type", "csv")
        delimiter = source.get("parameters", {}).get("delimiter", ",")
        
        return f'''
def extract_data(**context):
    \"\"\"Spark-распределенное извлечение данных из Hadoop\"\"\"
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import spark_partition_id, monotonically_increasing_id
    
    try:
        # Инициализация Spark с распределенной конфигурацией
        spark = SparkSession.builder \\
            .appName("LCT_Distributed_Extract") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \\
            .getOrCreate()
        
        # Распределение на воркеры
        print("[SPARK] Инициализация распределенного извлечения данных")
        spark.sparkContext.setLogLevel("WARN")
        
        # Чтение данных из Hadoop с оптимизацией
        hadoop_path = "{hadoop_path}"
        print(f"[SPARK] Распределенное чтение из: {{hadoop_path}}")
        
        if "{source_type}" == "csv":
            # Оптимизированное чтение CSV с распределением
            df = spark.read \\
                .option("header", "true") \\
                .option("delimiter", "{delimiter}") \\
                .option("inferSchema", "true") \\
                .option("multiline", "true") \\
                .option("escape", "\\"") \\
                .option("maxFilesPerTrigger", "10") \\
                .csv(hadoop_path)
        elif "{source_type}" == "json":
            df = spark.read \\
                .option("multiline", "true") \\
                .option("mode", "PERMISSIVE") \\
                .json(hadoop_path)
        else:
            df = spark.read.text(hadoop_path)
        
        # Добавление метаданных для трекинга
        df_with_metadata = df \\
            .withColumn("partition_id", spark_partition_id()) \\
            .withColumn("record_id", monotonically_increasing_id())
        
        # Статистика распределения
        partition_count = df_with_metadata.rdd.getNumPartitions()
        row_count = df_with_metadata.count()
        
        print(f"[SPARK] Распределено на {{partition_count}} партиций, {{row_count}} записей")
        
        # Сохранение с партиционированием
        temp_path = "/tmp/spark_extracted_data"
        df_with_metadata.write \\
            .mode("overwrite") \\
            .option("maxRecordsPerFile", "100000") \\
            .parquet(temp_path)
        
        # XCom данные
        context['task_instance'].xcom_push(key='extracted_data_path', value=temp_path)
        context['task_instance'].xcom_push(key='row_count', value=row_count)
        context['task_instance'].xcom_push(key='partition_count', value=partition_count)
        context['task_instance'].xcom_push(key='spark_distributed', value=True)
        
        print("[SPARK] Распределенное извлечение завершено успешно")
        return temp_path
        
    except Exception as e:
        print(f"[SPARK ERROR] Ошибка распределенного извлечения: {{e}}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
'''
    
    def _generate_spark_transform_function(self, sources: List[Dict[str, Any]]) -> str:
        """Генерирует Spark-распределенную функцию трансформации"""
        
        return '''
def transform_data(**context):
    \"\"\"Spark-распределенная трансформация данных\"\"\"
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    
    try:
        # Инициализация Spark для распределенной трансформации
        spark = SparkSession.builder \\
            .appName("LCT_Distributed_Transform") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \\
            .config("spark.dynamicAllocation.enabled", "true") \\
            .getOrCreate()
        
        # Получение данных из предыдущего этапа
        extracted_path = context['task_instance'].xcom_pull(
            task_ids='extract_data', 
            key='extracted_data_path'
        )
        
        print(f"[SPARK] Распределенная трансформация данных из: {extracted_path}")
        
        # Загрузка данных
        df = spark.read.parquet(extracted_path)
        
        # Распределенные трансформации
        print("[SPARK] Применение распределенных AI трансформаций...")
        
        # 1. Параллельная очистка данных
        df_clean = df.dropna(how='all') \\
                    .filter(col("record_id").isNotNull())
        
        # 2. Распределенная обработка строковых полей
        string_columns = [f.name for f in df_clean.schema.fields 
                         if f.dataType == StringType() and f.name not in ["partition_id", "record_id"]]
        
        for col_name in string_columns:
            df_clean = df_clean.withColumn(
                col_name, 
                when(col(col_name).isNotNull(), 
                     trim(upper(col(col_name))))
                .otherwise(col(col_name))
            )
        
        # 3. Добавление метаданных обработки
        df_transformed = df_clean \\
            .withColumn("processed_at", current_timestamp()) \\
            .withColumn("processing_version", lit("2.0")) \\
            .withColumn("data_source", lit("spark_hadoop_pipeline")) \\
            .withColumn("transformation_method", lit("distributed"))
        
        # 4. Распределенная валидация
        original_count = context['task_instance'].xcom_pull(
            task_ids='extract_data', 
            key='row_count'
        )
        
        transformed_count = df_transformed.count()
        partition_count = df_transformed.rdd.getNumPartitions()
        
        data_quality_ratio = transformed_count / original_count if original_count > 0 else 0
        
        print(f"[SPARK] Распределенная обработка: {data_quality_ratio:.2%} ({transformed_count}/{original_count})")
        print(f"[SPARK] Партиций: {partition_count}")
        
        # Сохранение с оптимизацией
        transformed_path = "/tmp/spark_transformed_data"
        df_transformed.write \\
            .mode("overwrite") \\
            .option("maxRecordsPerFile", "50000") \\
            .partitionBy("partition_id") \\
            .parquet(transformed_path)
        
        # Метаданные
        context['task_instance'].xcom_push(key='transformed_data_path', value=transformed_path)
        context['task_instance'].xcom_push(key='transformed_count', value=transformed_count)
        context['task_instance'].xcom_push(key='data_quality_ratio', value=data_quality_ratio)
        context['task_instance'].xcom_push(key='spark_partitions', value=partition_count)
        
        print("[SPARK] Распределенная трансформация завершена")
        return transformed_path
        
    except Exception as e:
        print(f"[SPARK ERROR] Ошибка распределенной трансформации: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
'''

    def generate_pipeline_structure(self, 
                                  sources: List[Dict[str, Any]], 
                                  targets: List[Dict[str, Any]],
                                  request_uid: str,
                                  pipeline_uid: str) -> Dict[str, Any]:
        """Генерирует структуру пайплайнов согласно требованиям команды"""
        
        # Базовый шаблон DAG с информацией о Spark
        # Проверяем использование Spark для каждого источника
        spark_sources = [s for s in sources if self._should_use_spark_for_data(s)]
        has_spark = len(spark_sources) > 0
        
        if has_spark:
            spark_info = f"# SPARK ENABLED: Распределенное выполнение для {len(spark_sources)} источников данных (размер > 500MB или > 1M строк)\n"
        else:
            spark_info = "# SPARK DISABLED: Стандартное выполнение (все источники < 500MB и < 1M строк)\n"
        
        dag_template = f'''
{spark_info}from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Конфигурация DAG
default_args = {{
    'owner': 'lct-ai-service',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

# Создание DAG
dag = DAG(
    'hadoop_migration_pipeline{"_spark" if self.enable_spark_distribution else ""}',
    default_args=default_args,
    description='AI-генерированный пайплайн миграции данных из Hadoop{"с Spark распределением" if self.enable_spark_distribution else ""}',
    schedule_interval=None,  # Ручной запуск
    catchup=False,
    tags=['hadoop', 'migration', 'ai-generated'{"', 'spark-distributed'" if self.enable_spark_distribution else ""}]
)

# Задачи будут добавлены динамически через pipelines
'''
        
        # Генерируем пайплайны
        pipelines = []
        
        # Пайплайн 1: Создание схемы (level 0)
        schema_pipeline = {
            "level": 0,
            "id": str(uuid.uuid4()),
            "from": sources[0]["uid"] if sources else request_uid,
            "to": targets[0]["uid"] if targets else pipeline_uid,
            "function_name": "create_target_schema",
            "function_name_ru": "Создание схемы в таргете",
            "function_body": '''
def create_target_schema(**context):
    \"\"\"Создание целевой схемы в базе данных\"\"\"
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    
    try:
        # Подключение к БД для создания схемы
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="test",
            user="postgres",
            password="password"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Создание таблицы для мигрированных данных
        create_table_sql = \"\"\"
        CREATE SCHEMA IF NOT EXISTS public;
        
        DROP TABLE IF EXISTS public.migrated_data;
        
        CREATE TABLE public.migrated_data (
            id SERIAL PRIMARY KEY,
            data JSONB,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processing_version VARCHAR(10),
            data_source VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Индексы для оптимизации
        CREATE INDEX IF NOT EXISTS idx_migrated_data_processed_at 
        ON public.migrated_data(processed_at);
        
        CREATE INDEX IF NOT EXISTS idx_migrated_data_source 
        ON public.migrated_data(data_source);
        \"\"\"
        
        cursor.execute(create_table_sql)
        print("[SUCCESS] Целевая схема успешно создана")
        
        cursor.close()
        conn.close()
        
        return "schema_created"
        
    except Exception as e:
        print(f"[ERROR] Ошибка создания схемы: {e}")
        raise
'''
        }
        pipelines.append(schema_pipeline)
        
        # Пайплайн 2: Извлечение данных (level 1) - с опциональным Spark
        extract_function_body = (
            self._generate_spark_extract_function(sources[0] if sources else {})
            if self.enable_spark_distribution
            else self._generate_hadoop_extract_function(sources[0] if sources else {})
        )
        
        # Определяем, нужно ли Spark для этой функции
        is_spark_distributed = self._is_function_spark_distributed("extract_data", sources)
        
        extract_pipeline = {
            "level": 1,
            "id": str(uuid.uuid4()),
            "from": sources[0]["uid"] if sources else schema_pipeline["id"],
            "to": schema_pipeline["id"],
            "function_name": "extract_data",
            "function_name_ru": ("Spark-распределенное извлечение данных из Hadoop" 
                               if is_spark_distributed 
                               else "Извлечение данных из Hadoop"),
            "function_body": extract_function_body,
            "spark_distributed": is_spark_distributed
        }
        pipelines.append(extract_pipeline)
        
        # Пайплайн 3: Трансформация данных (level 2) - с опциональным Spark
        transform_function_body = (
            self._generate_spark_transform_function(sources)
            if self.enable_spark_distribution
            else self._generate_transform_function(sources)
        )
        
        # Определяем, нужно ли Spark для трансформации
        is_transform_spark_distributed = self._is_function_spark_distributed("transform_data", sources)
        
        transform_pipeline = {
            "level": 2,
            "id": str(uuid.uuid4()),
            "from": extract_pipeline["id"],
            "to": extract_pipeline["id"],
            "function_name": "transform_data",
            "function_name_ru": ("Spark-распределенная трансформация и очистка данных"
                               if is_transform_spark_distributed 
                               else "Трансформация и очистка данных"),
            "function_body": transform_function_body,
            "spark_distributed": is_transform_spark_distributed
        }
        pipelines.append(transform_pipeline)
        
        # Пайплайн 4: Загрузка данных (level 3)
        load_pipeline = {
            "level": 3,
            "id": str(uuid.uuid4()),
            "from": transform_pipeline["id"],
            "to": targets[0]["uid"] if targets else pipeline_uid,
            "function_name": "load_data",
            "function_name_ru": "Загрузка в целевую БД",
            "function_body": self._generate_load_function(targets[0] if targets else {})
        }
        pipelines.append(load_pipeline)
        
        # Пайплайн 5: Валидация (level 4)
        validate_pipeline = {
            "level": 4,
            "id": str(uuid.uuid4()),
            "from": load_pipeline["id"],
            "to": load_pipeline["id"],
            "function_name": "validate_data",
            "function_name_ru": "Валидация загруженных данных",
            "function_body": '''
def validate_data(**context):
    \"\"\"Валидация загруженных данных\"\"\"
    import psycopg2
    
    try:
        # Подключение к БД для валидации
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="test",
            user="postgres",
            password="password"
        )
        cursor = conn.cursor()
        
        # Проверка количества записей
        cursor.execute("SELECT COUNT(*) FROM public.migrated_data")
        count = cursor.fetchone()[0]
        
        # Получение ожидаемого количества из предыдущих задач
        expected_count = context['task_instance'].xcom_pull(
            task_ids='load_data', 
            key='loaded_count'
        )
        
        print(f"Загружено записей: {count}, ожидалось: {expected_count}")
        
        if count != expected_count:
            raise ValueError(f"Несоответствие количества записей: {count} != {expected_count}")
        
        # Проверка качества данных
        cursor.execute(\"\"\"
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT data) as unique_records,
                COUNT(*) FILTER (WHERE processed_at IS NOT NULL) as with_timestamp
            FROM public.migrated_data
        \"\"\")
        
        total, unique, with_ts = cursor.fetchone()
        quality_score = (unique / total) * 100 if total > 0 else 0
        
        print(f"Качество данных: {quality_score:.1f}% уникальных записей")
        
        context['task_instance'].xcom_push(key='validation_passed', value=True)
        context['task_instance'].xcom_push(key='quality_score', value=quality_score)
        
        cursor.close()
        conn.close()
        
        print("[SUCCESS] Валидация данных пройдена")
        return True
        
    except Exception as e:
        print(f"[ERROR] Ошибка валидации: {e}")
        raise
'''
        }
        pipelines.append(validate_pipeline)
        
        # Пайплайн 6: Финальный отчет (level 5)
        report_pipeline = {
            "level": 5,
            "id": str(uuid.uuid4()),
            "from": validate_pipeline["id"],
            "to": pipeline_uid,
            "function_name": "generate_report",
            "function_name_ru": "Генерация отчета миграции",
            "function_body": '''
def generate_report(**context):
    \"\"\"Генерация финального отчета миграции\"\"\"
    import json
    from datetime import datetime
    
    try:
        # Сбор метрик из всех задач
        extracted_count = context['task_instance'].xcom_pull(
            task_ids='extract_data', key='row_count'
        )
        transformed_count = context['task_instance'].xcom_pull(
            task_ids='transform_data', key='transformed_count'
        )
        loaded_count = context['task_instance'].xcom_pull(
            task_ids='load_data', key='loaded_count'
        )
        quality_score = context['task_instance'].xcom_pull(
            task_ids='validate_data', key='quality_score'
        )
        
        # Создание отчета
        report = {
            "migration_summary": {
                "pipeline_id": "''' + pipeline_uid + '''",
                "completed_at": datetime.now().isoformat(),
                "status": "SUCCESS",
                "data_flow": {
                    "extracted_from_hadoop": extracted_count,
                    "transformed": transformed_count,
                    "loaded_to_db": loaded_count,
                    "quality_score": f"{quality_score:.1f}%"
                },
                "performance": {
                    "data_loss_ratio": f"{((extracted_count - loaded_count) / extracted_count * 100):.1f}%" if extracted_count > 0 else "0%",
                    "transformation_efficiency": f"{(transformed_count / extracted_count * 100):.1f}%" if extracted_count > 0 else "0%"
                }
            }
        }
        
        print("[DATA] Отчет миграции:")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        
        context['task_instance'].xcom_push(key='final_report', value=report)
        
        print("[SUCCESS] Миграция завершена успешно!")
        return report
        
    except Exception as e:
        print(f"[ERROR] Ошибка генерации отчета: {e}")
        raise
'''
        }
        pipelines.append(report_pipeline)
        
        return {
            "template": dag_template.strip(),
            "pipelines": pipelines
        }


# Для тестирования
if __name__ == "__main__":
    generator = HadoopDAGGenerator()
    
    # Тестовые данные
    sample_sources = [{
        "uid": str(uuid.uuid4()),
        "parameters": {
            "type": "csv",
            "file_path": "hdfs://hadoop-cluster/pipeline_data/sample.csv",
            "delimiter": ";"
        }
    }]
    
    sample_targets = [{
        "uid": str(uuid.uuid4()),
        "parameters": {
            "type": "postgre_s_q_l",
            "host": "localhost",
            "port": 5432,
            "database": "test",
            "user": "postgres",
            "password": "password",
            "schema": "public"
        }
    }]
    
    # Генерация структуры пайплайнов
    result = generator.generate_pipeline_structure(
        sample_sources, 
        sample_targets,
        str(uuid.uuid4()),
        str(uuid.uuid4())
    )
    
    print(f"[SUCCESS] Сгенерирована структура с {len(result['pipelines'])} пайплайнами")
    print(f"[LIST] Уровни: {[p['level'] for p in result['pipelines']]}")
    print(f"[CONNECT] Функции: {[p['function_name'] for p in result['pipelines']]}")