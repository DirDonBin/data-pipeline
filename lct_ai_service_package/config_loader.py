import json
import os
from typing import Dict, Any, Optional

class Config:
    """Конфигурация сервиса из JSON файла"""
    
    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self._config_data = {}
        self.load_config()
    
    def load_config(self) -> None:
        """Загружает конфигурацию из JSON файла"""
        try:
            config_file_path = os.path.join(os.path.dirname(__file__), self.config_path)
            
            if not os.path.exists(config_file_path):
                print(f"Файл конфигурации {config_file_path} не найден!")
                self._load_default_config()
                return
            
            with open(config_file_path, 'r', encoding='utf-8') as file:
                self._config_data = json.load(file)
                
            print(f"Конфигурация загружена из {config_file_path}")
            
        except Exception as e:
            print(f"Ошибка загрузки конфигурации: {e}")
            self._load_default_config()
    
    def _load_default_config(self) -> None:
        """Загружает конфигурацию по умолчанию"""
        self._config_data = {
            "api": {"host": "0.0.0.0", "port": 8080},
            "deepseek": {"api_key": "", "model": "deepseek-chat"},
            "hadoop": {"cluster_url": "hdfs://hadoop-cluster"},
            "logging": {"level": "INFO"}
        }
        print("Используется конфигурация по умолчанию")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Получает значение по ключу (поддерживает точечную нотацию)"""
        keys = key.split('.')
        value = self._config_data
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """Получает всю секцию конфигурации"""
        return self._config_data.get(section, {})
    
    def update(self, key: str, value: Any) -> None:
        """Обновляет значение конфигурации"""
        keys = key.split('.')
        data = self._config_data
        
        for k in keys[:-1]:
            if k not in data:
                data[k] = {}
            data = data[k]
        
        data[keys[-1]] = value
    
    def save_config(self) -> None:
        """Сохраняет текущую конфигурацию в файл"""
        try:
            config_file_path = os.path.join(os.path.dirname(__file__), self.config_path)
            with open(config_file_path, 'w', encoding='utf-8') as file:
                json.dump(self._config_data, file, indent=2, ensure_ascii=False)
            print(f"Конфигурация сохранена в {config_file_path}")
        except Exception as e:
            print(f"Ошибка сохранения конфигурации: {e}")
    
    # Удобные методы для основных настроек
    @property
    def api_host(self) -> str:
        return self.get('api.host', '0.0.0.0')
    
    @property
    def api_port(self) -> int:
        return self.get('api.port', 8080)
    
    @property
    def deepseek_api_key(self) -> str:
        return self.get('deepseek.api_key', '')
    
    @property
    def deepseek_model(self) -> str:
        return self.get('deepseek.model', 'deepseek-chat')
    
    @property
    def deepseek_base_url(self) -> str:
        return self.get('deepseek.base_url', 'https://api.deepseek.com')
    
    @property
    def hadoop_cluster_url(self) -> str:
        return self.get('hadoop.cluster_url', 'hdfs://hadoop-cluster')
    
    @property
    def spark_master_url(self) -> str:
        return self.get('spark.master_url', 'spark://spark-master:7077')
    
    @property
    def log_level(self) -> str:
        return self.get('logging.level', 'INFO')

# Глобальный экземпляр конфигурации
config = Config()

# Функции для быстрого доступа
def get_config(key: str, default: Any = None) -> Any:
    """Быстрый доступ к конфигурации"""
    return config.get(key, default)

def get_api_config() -> Dict[str, Any]:
    """Получить конфигурацию API"""
    return config.get_section('api')

def get_deepseek_config() -> Dict[str, Any]:
    """Получить конфигурацию DeepSeek"""
    return config.get_section('deepseek')

def get_hadoop_config() -> Dict[str, Any]:
    """Получить конфигурацию Hadoop"""
    return config.get_section('hadoop')

def get_spark_config() -> Dict[str, Any]:
    """Получить конфигурацию Spark"""
    return config.get_section('spark')

if __name__ == "__main__":
    # Тест конфигурации
    print("Тестирование конфигурации:")
    print(f"API host: {config.api_host}")
    print(f"API port: {config.api_port}")
    print(f"DeepSeek model: {config.deepseek_model}")
    print(f"Hadoop URL: {config.hadoop_cluster_url}")
    print(f"Spark URL: {config.spark_master_url}")