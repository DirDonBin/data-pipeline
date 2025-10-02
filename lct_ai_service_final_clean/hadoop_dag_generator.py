"""
Hadoop DAG Generator для создания Airflow пайплайнов
Исправленная версия с поддержкой всех требований fix.md
"""
import uuid
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class HadoopDAGGenerator:
    """Генератор DAG файлов для Hadoop миграции"""
    
    def __init__(self, enable_spark_distribution: bool = False):
        self.enable_spark_distribution = enable_spark_distribution
        logger.info(f"HadoopDAGGenerator инициализирован, Spark: {'включен' if enable_spark_distribution else 'отключен'}")
    
    def generate_pipeline_structure(self, sources: List[Dict], targets: List[Dict], 
                                  request_uid: str, pipeline_uid: str,
                                  target_recommendations: List[Dict] = None) -> Dict[str, Any]:
        """Генерирует структуру пайплайна с nodes и connections"""
        
        # Обрабатываем рекомендации по хранилищам
        if target_recommendations is None:
            target_recommendations = []
            
        # Вспомогательная функция для получения рекомендации по хранилищу
        def get_target_storage_info(node_type: str):
            if target_recommendations:
                # Берем первую рекомендацию как основную
                primary_rec = target_recommendations[0]
                return {
                    "target_storage": primary_rec.get("primary"),  # Без fallback
                    "target_reasoning": primary_rec.get("reasoning"),  # Без fallback
                    "storage_characteristics": primary_rec.get("characteristics", {})
                }
            else:
                # Если нет рекомендаций - не добавляем target информацию
                return {}
        
        # Создаем базовый template DAG
        dag_template = self._generate_dag_template()
        
        # Получаем целевое хранилище для использования в генерации кода
        primary_target_storage = None  # По умолчанию нет хранилища
        if target_recommendations:
            primary_target_storage = target_recommendations[0].get("primary")  # Без fallback
        
        # Генерируем ноды с их конфигурациями
        node_1_id = str(uuid.uuid4())
        node_2_id = str(uuid.uuid4())
        node_3_id = str(uuid.uuid4())
        node_4_id = str(uuid.uuid4())
        node_5_id = str(uuid.uuid4())
        node_6_id = str(uuid.uuid4())
        
        nodes = [
            {
                "id": node_1_id,
                "type": "schema",
                "name": "create_target_schema",
                "name_ru": "Создание схемы в таргете",
                "description": "Создание таблиц и структуры в целевой базе данных",
                "config": {
                    "level": 0,
                    "spark_distributed": False,
                    "from": request_uid,
                    "to": pipeline_uid
                },
                # Для обычных Python задач
                "code": self._generate_create_schema_function(),
                # Для Spark задач - 4 дополнительных поля
                "task_name": "create_schema_task",
                "dag_task_definition": self._generate_schema_dag_task(),
                "function_filename": "create_schema.py", 
                "function_code": self._generate_create_schema_function(),
                # Target storage information
                **get_target_storage_info("schema")
            },
            {
                "id": node_2_id,
                "type": "extract",
                "name": "extract_data",
                "name_ru": "Извлечение данных из Hadoop",
                "description": "Чтение и извлечение данных из исходного файла в Hadoop",
                "config": {
                    "level": 1,
                    "spark_distributed": self.enable_spark_distribution,
                    "from": str(uuid.uuid4()),
                    "to": str(uuid.uuid4())
                },
                # Для обычных Python задач
                "code": self._generate_extract_function(sources),
                # Для Spark задач
                "task_name": "spark_extract_task" if self.enable_spark_distribution else "extract_task",
                "dag_task_definition": self._generate_spark_extract_dag_task() if self.enable_spark_distribution else self._generate_python_extract_dag_task(),
                "function_filename": "spark_extract.py" if self.enable_spark_distribution else "extract.py",
                "function_code": self._generate_spark_extract_function() if self.enable_spark_distribution else self._generate_extract_function(sources),
                # Target storage information
                **get_target_storage_info("extract")
            },
            {
                "id": node_3_id,
                "type": "transform",
                "name": "transform_data",
                "name_ru": "Трансформация и очистка данных",
                "description": "Обработка, валидация и преобразование данных",
                "config": {
                    "level": 2,
                    "spark_distributed": self.enable_spark_distribution,
                    "from": str(uuid.uuid4()),
                    "to": str(uuid.uuid4())
                },
                "code": self._generate_transform_function(sources, targets),
                "task_name": "spark_transform_task" if self.enable_spark_distribution else "transform_task",
                "dag_task_definition": self._generate_spark_transform_dag_task() if self.enable_spark_distribution else self._generate_python_transform_dag_task(),
                "function_filename": "spark_transform.py" if self.enable_spark_distribution else "transform.py",
                "function_code": self._generate_spark_transform_function() if self.enable_spark_distribution else self._generate_transform_function(sources, targets),
                # Target storage information
                **get_target_storage_info("transform")
            },
            {
                "id": node_4_id,
                "type": "load",
                "name": "load_data",
                "name_ru": "Загрузка в целевую БД",
                "description": "Загрузка обработанных данных в целевую систему",
                "config": {
                    "level": 3,
                    "spark_distributed": self.enable_spark_distribution,
                    "from": str(uuid.uuid4()),
                    "to": pipeline_uid
                },
                "code": self._generate_load_function(targets, primary_target_storage),
                "task_name": "spark_load_task" if self.enable_spark_distribution else "load_task",
                "dag_task_definition": self._generate_spark_load_dag_task() if self.enable_spark_distribution else self._generate_python_load_dag_task(),
                "function_filename": "spark_load.py" if self.enable_spark_distribution else "load.py",
                "function_code": self._generate_spark_load_function(primary_target_storage) if self.enable_spark_distribution else self._generate_load_function(targets, primary_target_storage),
                # Target storage information
                **get_target_storage_info("load")
            },
            {
                "id": node_5_id,
                "type": "validation",
                "name": "validate_data",
                "name_ru": "Валидация загруженных данных",
                "description": "Проверка целостности и корректности загруженных данных",
                "config": {
                    "level": 4,
                    "spark_distributed": False,
                    "from": str(uuid.uuid4()),
                    "to": str(uuid.uuid4())
                },
                "code": self._generate_validation_function(),
                "task_name": "validate_task",
                "dag_task_definition": self._generate_python_validate_dag_task(),
                "function_filename": "validate.py",
                "function_code": self._generate_validation_function(),
                # Target storage information
                **get_target_storage_info("validation")
            },
            {
                "id": node_6_id,
                "type": "report",
                "name": "generate_report",
                "name_ru": "Генерация отчета миграции",
                "description": "Создание итогового отчета о выполненной миграции",
                "config": {
                    "level": 5,
                    "spark_distributed": False,
                    "from": str(uuid.uuid4()),
                    "to": pipeline_uid
                },
                "code": self._generate_report_function(),
                "task_name": "cleanup_task",
                "dag_task_definition": self._generate_python_cleanup_dag_task(),
                "function_filename": "cleanup.py",
                "function_code": self._generate_report_function(),
                # Target storage information
                **get_target_storage_info("report")
            }
        ]
        
        # Генерируем связи между нодами
        connections = [
            {"from": node_1_id, "to": node_2_id},
            {"from": node_2_id, "to": node_3_id},
            {"from": node_3_id, "to": node_4_id},
            {"from": node_4_id, "to": node_5_id},
            {"from": node_5_id, "to": node_6_id}
        ]
        
        return {
            "template": dag_template,
            "nodes": nodes,
            "connections": connections,
            # Оставляем старое поле pipelines для обратной совместимости с fix.md
            "pipelines": self._convert_nodes_to_pipelines(nodes),
            # Добавляем сборочные блоки для динамического DAG
            "dag_assembly": {
                "functions_block": self._generate_functions_block(nodes),
                "tasks_block": self._generate_tasks_block(nodes),
                "dependencies_block": self._generate_dependencies_block(connections)
            }
        }

    def _convert_nodes_to_pipelines(self, nodes: List[Dict]) -> List[Dict]:
        """Конвертирует новый формат nodes в старый формат pipelines для совместимости"""
        pipelines = []
        for node in nodes:
            pipeline = {
                "level": node["config"]["level"],
                "id": node["id"],
                "from": node["config"]["from"],
                "to": node["config"]["to"],
                "function_name": node["name"],
                "function_name_ru": node["name_ru"],
                "function_body": node["code"],
                "name": node["name_ru"],
                "description": node["description"],
                "spark_distributed": node["config"]["spark_distributed"]
            }
            pipelines.append(pipeline)
        return pipelines

    def _generate_functions_block(self, nodes: List[Dict]) -> str:
        """Генерирует блок с функциями для вставки в template"""
        functions = []
        for node in nodes:
            functions.append(f"# === {node['name_ru']} ===")
            functions.append(node['code'])
            functions.append("")  # Пустая строка между функциями
        return "\n".join(functions)

    def _generate_tasks_block(self, nodes: List[Dict]) -> str:
        """Генерирует блок с задачами Airflow для вставки в template"""
        tasks = []
        for node in nodes:
            task_id = node['name']
            task_name_ru = node['name_ru']
            
            task_definition = f'''
# {task_name_ru}
{task_id}_task = PythonOperator(
    task_id='{task_id}',
    python_callable={task_id},
    dag=dag
)'''
            tasks.append(task_definition)
        return "\n".join(tasks)

    def _generate_dependencies_block(self, connections: List[Dict]) -> str:
        """Генерирует блок с зависимостями между задачами"""
        dependencies = []
        dependencies.append("# Установка зависимостей между задачами")
        
        # Создаем мапинг node_id -> task_name
        node_to_task = {}
        for conn in connections:
            from_node = conn['from']
            to_node = conn['to']
            
            # Предполагаем, что task_name основан на node position
            # В реальности нужно будет передать nodes для корректного мапинга
            dependencies.append(f"# {from_node} >> {to_node}")
        
        # Пример простой последовательности
        task_names = ["create_target_schema", "extract_data", "transform_data", "load_data", "validate_data", "generate_report"]
        for i in range(len(task_names) - 1):
            dependencies.append(f"{task_names[i]}_task >> {task_names[i+1]}_task")
        
        return "\n".join(dependencies)

    def assemble_complete_dag(self, template: str, nodes: List[Dict], connections: List[Dict]) -> str:
        """Собирает полный DAG файл из template и компонентов"""
        
        # Генерируем блоки
        functions_block = self._generate_functions_block(nodes)
        tasks_block = self._generate_tasks_block(nodes)  
        dependencies_block = self._generate_dependencies_block(connections)
        
        # Заменяем плейсхолдеры в template
        complete_dag = template.replace("{{{{FUNCTIONS}}}}", functions_block)
        complete_dag = complete_dag.replace("{{{{TASK_SEQUENCE}}}}", tasks_block)
        complete_dag = complete_dag.replace("{{{{DEPENDENCIES}}}}", dependencies_block)
        
        return complete_dag

    # === МЕТОДЫ ДЛЯ ГЕНЕРАЦИИ DAG ЗАДАЧ ===
    
    def _generate_schema_dag_task(self) -> str:
        """Генерирует DAG задачу для создания схемы"""
        return '''create_schema_task = PythonOperator(
    task_id='create_target_schema',
    python_callable=create_target_schema,
    dag=dag
)'''

    def _generate_spark_extract_dag_task(self) -> str:
        """Генерирует Spark DAG задачу для извлечения"""
        return '''spark_extract_task = SparkSubmitOperator(
    task_id='spark_extract_data',
    application='/opt/airflow/dags/spark_jobs/spark_extract.py',
    name=SPARK_CONFIG["app_name"] + "_extract",
    conn_id='spark_default',
    verbose=SPARK_CONFIG["verbose"],
    dag=dag
)'''

    def _generate_python_extract_dag_task(self) -> str:
        """Генерирует Python DAG задачу для извлечения"""
        return '''extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)'''

    def _generate_spark_transform_dag_task(self) -> str:
        """Генерирует Spark DAG задачу для трансформации"""
        return '''spark_transform_task = SparkSubmitOperator(
    task_id='spark_transform_data',
    application='/opt/airflow/dags/spark_jobs/spark_transform.py',
    name=SPARK_CONFIG["app_name"] + "_transform",
    conn_id='spark_default',
    verbose=SPARK_CONFIG["verbose"],
    dag=dag
)'''

    def _generate_python_transform_dag_task(self) -> str:
        """Генерирует Python DAG задачу для трансформации"""
        return '''transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)'''

    def _generate_spark_load_dag_task(self) -> str:
        """Генерирует Spark DAG задачу для загрузки"""
        return '''spark_load_task = SparkSubmitOperator(
    task_id='spark_load_data',
    application='/opt/airflow/dags/spark_jobs/spark_load.py',
    name=SPARK_CONFIG["app_name"] + "_load",
    conn_id='spark_default',
    verbose=SPARK_CONFIG["verbose"],
    dag=dag
)'''

    def _generate_python_load_dag_task(self) -> str:
        """Генерирует Python DAG задачу для загрузки"""
        return '''load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)'''

    def _generate_python_validate_dag_task(self) -> str:
        """Генерирует Python DAG задачу для валидации"""
        return '''validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)'''

    def _generate_python_cleanup_dag_task(self) -> str:
        """Генерирует Python DAG задачу для очистки"""
        return '''cleanup_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)'''

    # === МЕТОДЫ ДЛЯ ГЕНЕРАЦИИ SPARK ФУНКЦИЙ ===
    
    def _generate_spark_extract_function(self) -> str:
        """Генерирует Spark функцию извлечения данных"""
        return '''from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \\
        .appName("DataExtraction") \\
        .getOrCreate()
    
    try:
        # Извлечение данных из Hadoop
        df = spark.read \\
            .option("header", "true") \\
            .option("inferSchema", "true") \\
            .csv("/hadoop/data/source.csv")
        
        # Сохранение для следующего шага
        df.write.mode("overwrite").parquet("/tmp/extracted_data")
        logger.info(f"Extracted {df.count()} records")
        
        return {"status": "success", "records": df.count()}
        
    except Exception as e:
        logger.error(f"Error in extraction: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()'''

    def _generate_spark_transform_function(self) -> str:
        """Генерирует Spark функцию трансформации данных"""
        return '''from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \\
        .appName("DataTransformation") \\
        .getOrCreate()
    
    try:
        # Чтение извлеченных данных
        df = spark.read.parquet("/tmp/extracted_data")
        
        # Трансформации данных
        transformed_df = df \\
            .filter(col("id").isNotNull()) \\
            .withColumn("processed_at", current_timestamp()) \\
            .withColumn("data_source", lit("hadoop_migration"))
        
        # Очистка и валидация
        clean_df = transformed_df \\
            .dropDuplicates() \\
            .filter(col("id") > 0)
        
        # Сохранение трансформированных данных
        clean_df.write.mode("overwrite").parquet("/tmp/transformed_data")
        logger.info(f"Transformed {clean_df.count()} records")
        
        return {"status": "success", "records": clean_df.count()}
        
    except Exception as e:
        logger.error(f"Error in transformation: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()'''

    def _generate_spark_load_function(self, target_storage: str = None) -> str:
        """Генерирует Spark функцию загрузки данных с учетом целевого хранилища"""
        
        if target_storage == "ClickHouse":
            return self._generate_spark_clickhouse_load()
        elif target_storage == "HDFS":
            return self._generate_spark_hdfs_load()
        else:  # PostgreSQL as default (включая None)
            return self._generate_spark_postgresql_load()
    
    def _generate_spark_postgresql_load(self) -> str:
        """Генерирует Spark функцию загрузки в PostgreSQL"""
        return '''from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \\
        .appName("DataLoading") \\
        .getOrCreate()
    
    try:
        # Чтение трансформированных данных
        df = spark.read.parquet("/tmp/transformed_data")
        
        # Загрузка в целевую систему
        df.write \\
            .format("jdbc") \\
            .option("url", "jdbc:postgresql://target-db:5432/analytics") \\
            .option("dbtable", "migrated_data") \\
            .option("user", "postgres") \\
            .option("password", "password") \\
            .option("driver", "org.postgresql.Driver") \\
            .mode("overwrite") \\
            .save()
        
        logger.info(f"Loaded {df.count()} records to target database")
        
        return {"status": "success", "records": df.count()}
        
    except Exception as e:
        logger.error(f"Error in loading: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()'''

    def _generate_spark_clickhouse_load(self) -> str:
        """Генерирует Spark функцию загрузки в ClickHouse"""
        return '''from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \\
        .appName("ClickHouseDataLoading") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
        .getOrCreate()
    
    try:
        # Чтение трансформированных данных
        df = spark.read.parquet("/tmp/transformed_data")
        
        # Оптимизация для ClickHouse - коалесцируем партиции
        df_optimized = df.coalesce(10)
        
        # Запись в промежуточный формат для ClickHouse
        clickhouse_staging = "/tmp/clickhouse_staging"
        df_optimized.write.mode("overwrite").parquet(clickhouse_staging)
        
        # ClickHouse connection и запись через JDBC
        jdbc_url = "jdbc:clickhouse://localhost:8123/analytics"
        
        df_optimized.write \\
            .format("jdbc") \\
            .option("url", jdbc_url) \\
            .option("dbtable", "migrated_data") \\
            .option("user", "default") \\
            .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \\
            .mode("overwrite") \\
            .save()
        
        logger.info(f"Loaded {df.count()} records to ClickHouse")
        
        return {"status": "success", "records": df.count(), "target": "ClickHouse"}
        
    except Exception as e:
        logger.error(f"Error in ClickHouse loading: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()'''

    def _generate_spark_hdfs_load(self) -> str:
        """Генерирует Spark функцию загрузки в HDFS"""
        return '''from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \\
        .appName("HDFSDataLoading") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \\
        .getOrCreate()
    
    try:
        # Чтение трансформированных данных
        df = spark.read.parquet("/tmp/transformed_data")
        
        # Партиционирование для HDFS
        df_partitioned = df.withColumn("year", df.timestamp.substr(1, 4)) \\
                          .withColumn("month", df.timestamp.substr(6, 2))
        
        # Запись в HDFS с оптимальным партиционированием
        hdfs_path = "/data/migrated/migrated_data"
        
        df_partitioned.write \\
            .mode("overwrite") \\
            .partitionBy("year", "month") \\
            .option("compression", "snappy") \\
            .parquet(hdfs_path)
        
        logger.info(f"Loaded {df.count()} records to HDFS: {hdfs_path}")
        
        return {"status": "success", "records": df.count(), "target": "HDFS"}
        
    except Exception as e:
        logger.error(f"Error in HDFS loading: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()'''
    
    def _generate_dag_template(self) -> str:
        """Генерирует динамический template DAG файла с плейсхолдерами"""
        spark_status = "ENABLED" if self.enable_spark_distribution else "DISABLED"
        comment = "Распределенное выполнение на Spark кластере" if self.enable_spark_distribution else "Стандартное выполнение (все источники < 500MB и < 1M строк)"
        
        return f'''# SPARK {spark_status}: {comment}
from datetime import datetime, timedelta
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
    'hadoop_migration_pipeline',
    default_args=default_args,
    description='AI-генерированный пайплайн миграции данных из Hadoop',
    schedule_interval=None,  # Ручной запуск
    catchup=False,
    tags=['hadoop', 'migration', 'ai-generated']
)

# ==== ДИНАМИЧЕСКИЕ ФУНКЦИИ ====
# Вставьте сюда функции из nodes[].code
{{{{FUNCTIONS}}}}

# ==== ДИНАМИЧЕСКАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ ЗАДАЧ ====  
# Вставьте сюда задачи на основе nodes и connections
{{{{TASK_SEQUENCE}}}}

# ==== УСТАНОВКА ЗАВИСИМОСТЕЙ ====
# Зависимости будут установлены на основе connections
{{{{DEPENDENCIES}}}}
'''

    def _generate_create_schema_function(self) -> str:
        """Генерирует функцию создания схемы"""
        return '''
def create_target_schema(**context):
    """Создание целевой схемы в базе данных"""
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
        create_table_sql = """
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
        """
        
        cursor.execute(create_table_sql)
        print("[SUCCESS] Целевая схема успешно создана")
        
        cursor.close()
        conn.close()
        
        return "schema_created"
        
    except Exception as e:
        print(f"[ERROR] Ошибка создания схемы: {e}")
        raise'''

    def _generate_extract_function(self, sources: List[Dict] = None) -> str:
        """Генерирует функцию извлечения данных из Hadoop"""
        # Получаем Hadoop пути из источников
        hadoop_paths = []
        if sources:
            for source in sources:
                hadoop_path = source.get("hadoop_file_path", "/hadoop/data/default.csv")
                if hadoop_path:
                    hadoop_paths.append(hadoop_path)
        
        if not hadoop_paths:
            hadoop_paths = ["/hadoop/data/default.csv"]
        
        return f'''
def extract_data(**context):
    """Извлечение данных из Hadoop файла"""
    from pyspark.sql import SparkSession
    import pandas as pd
    
    try:
        # Инициализация Spark сессии для работы с Hadoop
        spark = SparkSession.builder \\
            .appName("LCT_Data_Extract") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
            .getOrCreate()
        
        # Hadoop пути из запроса
        hadoop_paths = {hadoop_paths}
        print(f"Извлечение данных из Hadoop: {{hadoop_paths}}")
        
        extracted_data = []
        total_rows = 0
        
        for hadoop_path in hadoop_paths:
            print(f"Обработка файла: {{hadoop_path}}")
            
            # Определение типа файла по расширению
            if hadoop_path.endswith('.csv'):
                df = spark.read \\
                    .option("header", "true") \\
                    .option("delimiter", ";") \\
                    .option("inferSchema", "true") \\
                    .option("multiline", "true") \\
                    .option("escape", "\\"") \\
                    .csv(hadoop_path)
            elif hadoop_path.endswith('.json'):
                df = spark.read.json(hadoop_path)
            elif hadoop_path.endswith('.xml'):
                # Для XML требуется дополнительная библиотека
                df = spark.read \\
                    .format("com.databricks.spark.xml") \\
                    .option("rootTag", "root") \\
                    .option("rowTag", "record") \\
                    .load(hadoop_path)
            else:
                # По умолчанию CSV
                df = spark.read \\
                    .option("header", "true") \\
                    .option("delimiter", ";") \\
                    .csv(hadoop_path)
            
            # Подсчет записей
            row_count = df.count()
            total_rows += row_count
            print(f"Извлечено записей из {{hadoop_path}}: {{row_count}}")
            
            if row_count == 0:
                print(f"[WARNING] Файл {{hadoop_path}} пуст")
                continue
            
            extracted_data.append({{
                "path": hadoop_path,
                "rows": row_count,
                "schema": df.schema.json()
            }})
        
        if total_rows == 0:
            raise ValueError("Все файлы пусты или не содержат данных")
        
        # Сохранение во временное хранилище для следующих задач
        temp_path = "/tmp/extracted_data.parquet"
        if len(hadoop_paths) > 1:
            # Объединение нескольких источников
            dfs = []
            for hadoop_path in hadoop_paths:
                if hadoop_path.endswith('.csv'):
                    df = spark.read.option("header", "true").option("delimiter", ";").csv(hadoop_path)
                elif hadoop_path.endswith('.json'):
                    df = spark.read.json(hadoop_path)
                else:
                    df = spark.read.option("header", "true").csv(hadoop_path)
                dfs.append(df)
            
            # Объединение по схеме (добавляем source_file колонку)
            combined_df = None
            for i, df in enumerate(dfs):
                df_with_source = df.withColumn("source_file", lit(hadoop_paths[i]))
                if combined_df is None:
                    combined_df = df_with_source
                else:
                    combined_df = combined_df.unionByName(df_with_source, allowMissingColumns=True)
            
            combined_df.write.mode("overwrite").parquet(temp_path)
        else:
            # Один источник
            hadoop_path = hadoop_paths[0]
            if hadoop_path.endswith('.csv'):
                df = spark.read.option("header", "true").option("delimiter", ";").csv(hadoop_path)
            elif hadoop_path.endswith('.json'):
                df = spark.read.json(hadoop_path)
            else:
                df = spark.read.option("header", "true").csv(hadoop_path)
            
            df.write.mode("overwrite").parquet(temp_path)
        
        # Передача метаданных в XCom
        context['task_instance'].xcom_push(
            key='extracted_data_path', 
            value=temp_path
        )
        context['task_instance'].xcom_push(
            key='row_count', 
            value=total_rows
        )
        context['task_instance'].xcom_push(
            key='sources_info', 
            value=extracted_data
        )
        
        print(f"[SUCCESS] Данные успешно извлечены из Hadoop: {{total_rows}} записей")
        return temp_path
        
    except Exception as e:
        print(f"[ERROR] Ошибка извлечения данных: {{e}}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()'''

    def _generate_transform_function(self, sources: List[Dict], targets: List[Dict]) -> str:
        """Генерирует функцию трансформации данных"""
        return '''
def transform_data(**context):
    """Трансформация данных с поддержкой Spark"""
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
            spark.stop()'''

    def _generate_load_function(self, targets: List[Dict], target_storage: str = None) -> str:
        """Генерирует функцию загрузки данных с учетом целевого хранилища"""
        
        if target_storage == "ClickHouse":
            return self._generate_clickhouse_load_function(targets)
        elif target_storage == "HDFS":
            return self._generate_hdfs_load_function(targets)
        else:  # PostgreSQL as default (включая None)
            return self._generate_postgresql_load_function(targets)
    
    def _generate_postgresql_load_function(self, targets: List[Dict]) -> str:
        """Генерирует функцию загрузки в PostgreSQL"""
        return '''
def load_data(**context):
    """Загрузка данных в PostgreSQL"""
    from pyspark.sql import SparkSession
    import psycopg2
    from sqlalchemy import create_engine
    
    try:
        # Параметры подключения к PostgreSQL
        db_config = {
            "host": "localhost",
            "port": 5432,
            "database": "test",
            "user": "postgres",
            "password": "password",
            "schema": "public"
        }
        
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
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
        
        # Запись данных в БД
        df.write \\
            .format("jdbc") \\
            .option("url", jdbc_url) \\
            .option("dbtable", f"{db_config['schema']}.migrated_data") \\
            .option("user", db_config['user']) \\
            .option("password", db_config['password']) \\
            .option("driver", "org.postgresql.Driver") \\
            .mode("overwrite") \\
            .save()
        
        loaded_count = df.count()
        
        # Метаданные загрузки
        context['task_instance'].xcom_push(key='loaded_count', value=loaded_count)
        context['task_instance'].xcom_push(key='target_table', value=f"{db_config['schema']}.migrated_data")
        
        print(f"[SUCCESS] Загружено записей: {loaded_count}")
        return loaded_count
        
    except Exception as e:
        print(f"[ERROR] Ошибка загрузки: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()'''

    def _generate_clickhouse_load_function(self, targets: List[Dict]) -> str:
        """Генерирует функцию загрузки в ClickHouse"""
        return '''
def load_data(**context):
    """Загрузка данных в ClickHouse для аналитических запросов"""
    from pyspark.sql import SparkSession
    import clickhouse_connect
    
    try:
        # Инициализация Spark для аналитических задач
        spark = SparkSession.builder \\
            .appName("ClickHouseDataLoad") \\
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
            .getOrCreate()
        
        # Параметры подключения к ClickHouse
        ch_config = {
            "host": "localhost",
            "port": 8123,
            "database": "analytics",
            "user": "default",
            "password": ""
        }
        
        transformed_path = "/tmp/transformed_data"
        df = spark.read.parquet(transformed_path)
        
        # Оптимизация для ClickHouse - колумнарное хранение
        df_optimized = df.coalesce(10)  # Меньше файлов для ClickHouse
        
        # Сохранение в промежуточный формат
        temp_path = "/tmp/clickhouse_staging"
        df_optimized.write.mode("overwrite").parquet(temp_path)
        
        # Подключение к ClickHouse
        client = clickhouse_connect.get_client(
            host=ch_config['host'],
            port=ch_config['port'],
            database=ch_config['database'],
            username=ch_config['user'],
            password=ch_config['password']
        )
        
        # Создание таблицы ClickHouse с MergeTree движком
        create_table_sql = \"\"\"
        CREATE TABLE IF NOT EXISTS migrated_data (
            id UInt64,
            data String,
            timestamp DateTime64,
            category LowCardinality(String)
        ) ENGINE = MergeTree()
        ORDER BY (id, timestamp)
        PARTITION BY toYYYYMM(timestamp)
        SETTINGS index_granularity = 8192
        \"\"\"
        
        client.command(create_table_sql)
        
        # Загрузка данных batch-ами для оптимальной производительности
        pandas_df = df_optimized.toPandas()
        client.insert_df('migrated_data', pandas_df)
        
        loaded_count = len(pandas_df)
        print(f"[SUCCESS] Загружено {loaded_count} записей в ClickHouse")
        
        return f"clickhouse_loaded_{loaded_count}"
        
    except Exception as e:
        print(f"[ERROR] Ошибка загрузки в ClickHouse: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()'''

    def _generate_hdfs_load_function(self, targets: List[Dict]) -> str:
        """Генерирует функцию загрузки в HDFS"""
        return '''
def load_data(**context):
    """Загрузка данных в HDFS для больших объемов"""
    from pyspark.sql import SparkSession
    from hdfs import InsecureClient
    
    try:
        # Инициализация Spark для больших данных
        spark = SparkSession.builder \\
            .appName("HDFSDataLoad") \\
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \\
            .getOrCreate()
        
        # Параметры HDFS
        hdfs_config = {
            "url": "http://localhost:9870",
            "user": "hadoop",
            "root": "/data/migrated"
        }
        
        transformed_path = "/tmp/transformed_data"
        df = spark.read.parquet(transformed_path)
        
        # Оптимизация для HDFS - партиционирование и сжатие
        target_hdfs_path = f"{hdfs_config['root']}/migrated_data"
        
        # Партиционирование по дате для оптимального доступа
        df_partitioned = df.withColumn("year", df.timestamp.substr(1, 4)) \\
                          .withColumn("month", df.timestamp.substr(6, 2))
        
        # Запись в HDFS с оптимальными настройками
        df_partitioned.write \\
            .mode("overwrite") \\
            .partitionBy("year", "month") \\
            .option("compression", "snappy") \\
            .parquet(target_hdfs_path)
        
        # Проверка через HDFS API
        client = InsecureClient(hdfs_config['url'], user=hdfs_config['user'])
        
        # Получаем статистику загруженного
        loaded_files = list(client.walk(target_hdfs_path))
        loaded_count = df.count()
        
        print(f"[SUCCESS] Загружено {loaded_count} записей в HDFS: {target_hdfs_path}")
        print(f"[INFO] Создано файлов: {len(loaded_files)}")
        
        return f"hdfs_loaded_{loaded_count}"
        
    except Exception as e:
        print(f"[ERROR] Ошибка загрузки в HDFS: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()'''

    def _generate_validate_function(self) -> str:
        """Генерирует функцию валидации данных"""
        return '''
def validate_data(**context):
    """Валидация загруженных данных"""
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
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT data) as unique_records,
                COUNT(*) FILTER (WHERE processed_at IS NOT NULL) as with_timestamp
            FROM public.migrated_data
        """)
        
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
        raise'''

    def _generate_validation_function(self) -> str:
        """Генерирует функцию валидации данных"""
        return '''
def validate_data(**context):
    """Валидация загруженных данных"""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    try:
        # Подключение к БД для валидации
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="test",
            user="postgres",
            password="password"
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Проверка количества записей
        cursor.execute("SELECT COUNT(*) as total FROM public.migrated_data")
        result = cursor.fetchone()
        total_records = result['total']
        
        print(f"[INFO] Найдено {total_records} записей для валидации")
        
        # Проверка структуры данных
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'migrated_data'
        """)
        columns = cursor.fetchall()
        print(f"[INFO] Структура таблицы: {len(columns)} колонок")
        
        # Проверка целостности данных
        cursor.execute("""
            SELECT COUNT(*) as invalid_count 
            FROM public.migrated_data 
            WHERE data IS NULL OR data = '{}'::jsonb
        """)
        invalid_result = cursor.fetchone()
        invalid_count = invalid_result['invalid_count']
        
        if invalid_count > 0:
            print(f"[WARNING] Найдено {invalid_count} записей с некорректными данными")
        
        cursor.close()
        conn.close()
        
        validation_result = {
            "total_records": total_records,
            "invalid_records": invalid_count,
            "validation_status": "completed",
            "data_quality_score": max(0, 100 - (invalid_count / max(total_records, 1)) * 100)
        }
        
        print(f"[SUCCESS] Валидация завершена: {validation_result}")
        return validation_result
        
    except Exception as e:
        print(f"[ERROR] Ошибка валидации: {e}")
        raise'''

    def _generate_report_function(self) -> str:
        """Генерирует функцию создания отчета"""
        return '''
def generate_report(**context):
    """Генерация финального отчета миграции"""
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
                "pipeline_id": "01999ff0-dc7a-7465-9db8-ccd71d27e3a7",
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
        raise'''
