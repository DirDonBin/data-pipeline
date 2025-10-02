"""
Загрузчик конфигурации для LCT AI Service
"""
import json
import os
from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class Config:
    api_host: str
    api_port: int
    api_reload: bool
    log_level: str
    deepseek_api_key: str
    deepseek_base_url: str
    deepseek_model: str
    deepseek_max_tokens: int
    deepseek_temperature: float
    deepseek_timeout: int

def load_config() -> Config:
    """Загружает конфигурацию из config.json"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Файл конфигурации не найден: {config_path}")
    
    print(f"Конфигурация загружена из {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = json.load(f)
    
    return Config(
        api_host=config_data['api']['host'],
        api_port=config_data['api']['port'],
        api_reload=config_data['api']['reload'],
        log_level=config_data['api']['log_level'],
        deepseek_api_key=config_data['deepseek']['api_key'],
        deepseek_base_url=config_data['deepseek']['base_url'],
        deepseek_model=config_data['deepseek']['model'],
        deepseek_max_tokens=config_data['deepseek']['max_tokens'],
        deepseek_temperature=config_data['deepseek']['temperature'],
        deepseek_timeout=config_data['deepseek']['timeout']
    )

def get_config(path: str, default: Any = None) -> Any:
    """Получает значение конфигурации по пути (например, 'api.host')"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        keys = path.split('.')
        value = config_data
        for key in keys:
            value = value[key]
        return value
    except (KeyError, FileNotFoundError):
        return default

def get_api_config() -> Dict[str, Any]:
    """Получает API конфигурацию"""
    return get_config('api', {})

# Глобальная конфигурация
config = load_config()
