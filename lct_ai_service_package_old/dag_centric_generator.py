"""
DAG-центричный генератор для команды ЛЦТ
Все трансформации и миграции выполняются внутри DAG, а не отдельными скриптами
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import json


@dataclass
class DAGTaskConfig:
    """Конфигурация задачи DAG"""
    task_id: str
    task_type: str  # extract, transform, load, validate
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]
    transformation_rules: List[Dict[str, Any]]


class DAGCentricGenerator:
    """Генератор DAG с встроенными трансформациями базы данных"""
    
    def __init__(self, llm_service=None):
        self.llm_service = llm_service
    
    def generate_complete_dag(self, 
                            pipeline_uid: str,
                            sources: List[Dict[str, Any]], 
                            targets: List[Dict[str, Any]],
                            storage_recommendation: Dict[str, Any]) -> str:
        """
        Генерирует полный DAG с включенными трансформациями базы данных
        Никаких отдельных DDL или миграционных скриптов!
        """
        
        dag_code = self._generate_dag_header(pipeline_uid)
        dag_code += self._generate_imports()
        dag_code += self._generate_connection_configs(sources, targets)
        dag_code += self._generate_schema_tasks(sources, targets, storage_recommendation)
        dag_code += self._generate_data_pipeline_tasks(sources, targets, storage_recommendation)
        dag_code += self._generate_dag_structure(pipeline_uid)
        
        return dag_code
    
    def _generate_dag_header(self, pipeline_uid: str) -> str:
        """Генерирует заголовок DAG"""
        return f'''"""
AI Generated Data Pipeline DAG
Pipeline ID: {pipeline_uid}
Generated: {datetime.now().isoformat()}

Этот DAG включает:
- Создание схемы целевой БД
- Извлечение данных из источников  
- Трансформации данных
- Загрузку в целевое хранилище
- Валидацию результатов
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

'''
    
    def _generate_imports(self) -> str:
        """Генерирует дополнительные импорты"""
        return '''
# Дополнительные импорты для работы с данными
import sqlalchemy
from sqlalchemy import create_engine, text
import psycopg2
from typing import Dict, List, Any
import os

'''
    
    def _generate_connection_configs(self, sources: List[Dict], targets: List[Dict]) -> str:
        """Генерирует конфигурации подключений"""
        code = '''
# Конфигурации подключений
CONNECTIONS = {
'''
        
        # Источники
        for i, source in enumerate(sources):
            if source['type'] == 'File':
                code += f'''    "source_{i}": {{
        "type": "file",
        "format": "{source['parameters']['type'].lower()}",
        "path": "{source['parameters']['filepath']}",
        "delimiter": "{source['parameters'].get('delimiter', ',')}"
    }},
'''
        
        # Цели
        for i, target in enumerate(targets):
            if target['type'] == 'Database':
                params = target['parameters']
                code += f'''    "target_{i}": {{
        "type": "database",
        "engine": "{params['type'].lower()}",
        "host": "{params['host']}",
        "port": {params['port']},
        "database": "{params['database']}",
        "user": "{params['user']}",
        "password": "{params['password']}"
    }},
'''
        
        code += '''
}

'''
        return code
    
    def _generate_schema_tasks(self, sources: List[Dict], targets: List[Dict], storage_rec: Dict) -> str:
        """Генерирует задачи создания схемы БД внутри DAG"""
        code = '''
def create_target_schema(**context):
    """Создает схему в целевой базе данных"""
    logger.info("🏗️ Создание схемы целевой базы данных...")
    
    target_config = CONNECTIONS["target_0"]
    
    # Подключение к целевой БД
    if target_config["engine"] == "postgresql":
        hook = PostgresHook(postgres_conn_id="postgres_default")
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        # DDL команды встроены прямо в DAG
'''
        
        # Генерируем DDL команды на основе источников
        if sources:
            source_schema = sources[0].get('schema-infos', [{}])[0]
            fields = source_schema.get('fields', [])
            
            code += '''
        # Создаем таблицу на основе анализа источника
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS migrated_data (
'''
            
            for field in fields:
                pg_type = "TEXT"  # По умолчанию
                if field['data_type'] == 'Integer':
                    pg_type = "INTEGER"
                elif field['data_type'] == 'Float':
                    pg_type = "DECIMAL"
                elif field['data_type'] == 'DateTime':
                    pg_type = "TIMESTAMP"
                
                nullable = "NULL" if field['nullable'] else "NOT NULL"
                code += f'            {field["name"]} {pg_type} {nullable},\n'
            
            code += '''            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_sql)
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Схема базы данных создана успешно")
    
    else:
        raise ValueError(f"Неподдерживаемый тип БД: {target_config['engine']}")

'''
        
        return code
    
    def _generate_data_pipeline_tasks(self, sources: List[Dict], targets: List[Dict], storage_rec: Dict) -> str:
        """Генерирует задачи извлечения, трансформации и загрузки данных"""
        
        code = '''
def extract_data_from_source(**context):
    """Извлекает данные из источника"""
    logger.info("📥 Извлечение данных из источника...")
    
    source_config = CONNECTIONS["source_0"]
    
    if source_config["type"] == "file":
        if source_config["format"] == "csv":
            df = pd.read_csv(
                source_config["path"], 
                delimiter=source_config["delimiter"]
            )
        elif source_config["format"] == "json":
            df = pd.read_json(source_config["path"])
        else:
            raise ValueError(f"Неподдерживаемый формат: {source_config['format']}")
        
        logger.info(f"✅ Извлечено {len(df)} записей из {source_config['format'].upper()}")
        
        # Сохраняем во временную таблицу или файл для следующего шага
        df.to_csv("/tmp/extracted_data.csv", index=False)
        
        return {"rows_extracted": len(df), "columns": list(df.columns)}
    
    else:
        raise ValueError(f"Неподдерживаемый тип источника: {source_config['type']}")


def transform_data(**context):
    """Трансформирует данные согласно AI-рекомендациям"""
    logger.info("🔄 Трансформация данных...")
    
    # Читаем извлеченные данные
    df = pd.read_csv("/tmp/extracted_data.csv")
    
    # AI-рекомендованные трансформации
'''
        
        # Генерируем трансформации на основе LLM анализа
        if self.llm_service and sources:
            # Здесь можно добавить LLM-генерированные правила трансформации
            code += '''
    # Применяем AI-рекомендованные трансформации
    # Очистка данных
    df = df.dropna()  # Удаляем пустые строки
    
    # Нормализация строковых полей
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    
    # Преобразование типов данных
'''
            
            if sources:
                fields = sources[0].get('schema-infos', [{}])[0].get('fields', [])
                for field in fields:
                    if field['data_type'] == 'Integer':
                        code += f"    df['{field['name']}'] = pd.to_numeric(df['{field['name']}'], errors='coerce')\n"
                    elif field['data_type'] == 'DateTime':
                        code += f"    df['{field['name']}'] = pd.to_datetime(df['{field['name']}'], errors='coerce')\n"
        
        code += '''
    # Добавляем метаданные
    df['created_at'] = datetime.now()
    
    # Сохраняем трансформированные данные
    df.to_csv("/tmp/transformed_data.csv", index=False)
    
    logger.info(f"✅ Данные трансформированы. Итого записей: {len(df)}")
    
    return {"rows_transformed": len(df)}


def load_data_to_target(**context):
    """Загружает данные в целевое хранилище"""
    logger.info("📤 Загрузка данных в целевое хранилище...")
    
    # Читаем трансформированные данные
    df = pd.read_csv("/tmp/transformed_data.csv")
    
    target_config = CONNECTIONS["target_0"]
    
    if target_config["engine"] == "postgresql":
        # Создаем строку подключения
        connection_string = f"postgresql://{target_config['user']}:{target_config['password']}@{target_config['host']}:{target_config['port']}/{target_config['database']}"
        
        # Загружаем данные
        engine = create_engine(connection_string)
        df.to_sql('migrated_data', engine, if_exists='append', index=False)
        
        logger.info(f"✅ Загружено {len(df)} записей в PostgreSQL")
        
    else:
        raise ValueError(f"Неподдерживаемый тип целевой БД: {target_config['engine']}")
    
    return {"rows_loaded": len(df)}


def validate_migration(**context):
    """Валидирует результаты миграции"""
    logger.info("🔍 Валидация результатов миграции...")
    
    target_config = CONNECTIONS["target_0"]
    
    if target_config["engine"] == "postgresql":
        hook = PostgresHook(postgres_conn_id="postgres_default")
        
        # Проверяем количество загруженных записей
        result = hook.get_first("SELECT COUNT(*) FROM migrated_data")[0]
        logger.info(f"📊 В целевой таблице: {result} записей")
        
        # Дополнительные проверки качества данных
        null_checks = hook.get_first("SELECT COUNT(*) FROM migrated_data WHERE created_at IS NULL")[0]
        if null_checks > 0:
            logger.warning(f"⚠️ Найдено {null_checks} записей с пустой датой создания")
        
        logger.info("✅ Валидация завершена успешно")
        
        return {
            "total_records": result,
            "validation_errors": null_checks,
            "migration_status": "success" if null_checks == 0 else "warning"
        }
    
    else:
        raise ValueError(f"Неподдерживаемая БД для валидации: {target_config['engine']}")

'''
        
        return code
    
    def _generate_dag_structure(self, pipeline_uid: str) -> str:
        """Генерирует структуру DAG с задачами"""
        
        return f'''
# Конфигурация DAG
default_args = {{
    'owner': 'ai-data-engineer',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

# Создание DAG
dag = DAG(
    'pipeline_{pipeline_uid}',
    default_args=default_args,
    description='AI Generated Data Migration Pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['ai-generated', 'data-migration'],
)

# Определение задач
create_schema_task = PythonOperator(
    task_id='create_target_schema',
    python_callable=create_target_schema,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_source,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_target,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_migration',
    python_callable=validate_migration,
    dag=dag,
)

# Определение порядка выполнения задач
create_schema_task >> extract_task >> transform_task >> load_task >> validate_task

# DAG готов к выполнению в Airflow!
'''

# Пример использования генератора
def example_dag_generation():
    """Пример генерации DAG-центричного пайплайна"""
    
    generator = DAGCentricGenerator()
    
    # Пример источников и целей (формат команды ЛЦТ)
    sources = [{
        "type": "File",
        "parameters": {
            "type": "CSV",
            "filepath": "/data/source.csv",
            "delimiter": ","
        },
        "schema-infos": [{
            "size_mb": 100.0,
            "row_count": 50000,
            "fields": [
                {
                    "name": "id",
                    "data_type": "Integer",
                    "nullable": False,
                    "sample_values": ["1", "2", "3"],
                    "unique_values": 50000,
                    "null_count": 0,
                    "statistics": {"min": "1", "max": "50000", "avg": "25000"}
                },
                {
                    "name": "name", 
                    "data_type": "String",
                    "nullable": True,
                    "sample_values": ["John", "Jane", "Bob"],
                    "unique_values": 45000,
                    "null_count": 100,
                    "statistics": {"min_length": 3, "max_length": 20, "avg_length": 8}
                }
            ]
        }]
    }]
    
    targets = [{
        "type": "Database",
        "parameters": {
            "type": "PostgreSQL",
            "host": "localhost",
            "port": 5432,
            "database": "target_db",
            "user": "postgres",
            "password": "password"
        }
    }]
    
    storage_recommendation = {
        "type": "PostgreSQL",
        "confidence": 0.9,
        "reasoning": "Оптимально для структурированных данных"
    }
    
    # Генерируем DAG
    dag_code = generator.generate_complete_dag(
        pipeline_uid="example_123",
        sources=sources,
        targets=targets, 
        storage_recommendation=storage_recommendation
    )
    
    print("Сгенерированный DAG:")
    print(dag_code)
    
    return dag_code

if __name__ == "__main__":
    example_dag_generation()