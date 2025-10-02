"""
LLM сервис для интеграции с DeepSeek API
"""

import json
import logging
import os
from typing import Dict, Any, Optional
import requests

logger = logging.getLogger(__name__)

class LLMService:
    """LLM сервис с интеграцией DeepSeek API"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('DEEPSEEK_API_KEY', 'sk-a4ad2cc3a41e4b5581e82b05a2983a4b')
        self.base_url = "https://api.deepseek.com/v1/chat/completions"
        logger.info(f"LLM Service инициализирован {'с DeepSeek API' if self.api_key != 'demo_key' else '(demo mode)'}")
    
    def _call_deepseek_api(self, messages: list) -> str:
        """Вызов DeepSeek API"""
        if self.api_key == 'demo_key' or not self.api_key:
            return "DEMO: DeepSeek API response placeholder"
            
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "deepseek-chat",
            "messages": messages,
            "max_tokens": 1000,
            "temperature": 0.3
        }
        
        try:
            response = requests.post(self.base_url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"DeepSeek API error: {e}")
            return f"AI анализ временно недоступен: {str(e)}"
    
    def get_storage_recommendation(self, 
                                 sources: list, 
                                 targets: list, 
                                 business_context: str = None) -> Dict[str, Any]:
        """
        Получение рекомендаций по хранилищу данных через DeepSeek API
        """
        
        # Подготавливаем контекст для AI
        context = f"""
        Проанализируй следующие данные и дай рекомендации по оптимальному хранилищу данных:

        ИСТОЧНИКИ ДАННЫХ:
        {json.dumps(sources, ensure_ascii=False, indent=2)}

        ЦЕЛЕВЫЕ СИСТЕМЫ:
        {json.dumps(targets, ensure_ascii=False, indent=2)}

        БИЗНЕС-КОНТЕКСТ: {business_context or 'Не указан'}

        Верни ответ в JSON формате:
        {{
            "storage_type": "PostgreSQL|ClickHouse|MongoDB|Redis",
            "confidence": 0.95,
            "reasoning": "подробное обоснование выбора",
            "transformation_steps": ["шаг 1", "шаг 2"],
            "performance_estimate": "время выполнения",
            "cost_estimate": "относительная стоимость"
        }}
        """
        
        messages = [
            {
                "role": "system", 
                "content": "Ты - ведущий эксперт по архитектуре данных и ETL процессам. Анализируй данные и давай конкретные технические рекомендации с обоснованием. Отвечай только в JSON формате."
            },
            {
                "role": "user", 
                "content": context
            }
        ]
        
        ai_response = self._call_deepseek_api(messages)
        
        # Парсим ответ от AI
        try:
            # Пытаемся распарсить JSON ответ от AI
            if ai_response.startswith("```json"):
                ai_response = ai_response.replace("```json", "").replace("```", "").strip()
            
            ai_recommendation = json.loads(ai_response)
            
            # Валидируем обязательные поля
            required_fields = ["storage_type", "confidence", "reasoning"]
            if all(field in ai_recommendation for field in required_fields):
                return ai_recommendation
            else:
                logger.warning("AI ответ не содержит всех обязательных полей, используем fallback")
                
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Ошибка парсинга AI ответа: {e}, ответ: {ai_response}")
        
        # Fallback логика если AI недоступен или вернул некорректный ответ
        total_size_mb = sum(source.get('size_mb', 0) for source in sources if isinstance(source, dict))
        
        if total_size_mb > 1000:
            return {
                "storage_type": "ClickHouse",
                "confidence": 0.85,
                "reasoning": f"Большой объем данных ({total_size_mb}MB) - ClickHouse оптимален для аналитических нагрузок",
                "transformation_steps": ["Создание партиций по датам", "Индексирование ключевых полей", "Оптимизация типов данных"],
                "performance_estimate": "~2-3 минуты для обработки",
                "cost_estimate": "Средняя"
            }
        elif total_size_mb > 100:
            return {
                "storage_type": "PostgreSQL",
                "confidence": 0.90,
                "reasoning": f"Средний объем данных ({total_size_mb}MB) - PostgreSQL идеален для структурированных данных",
                "transformation_steps": ["Нормализация схемы", "Создание индексов", "Настройка constraints"],
                "performance_estimate": "~30-60 секунд для обработки",
                "cost_estimate": "Низкая"
            }
        else:
            return {
                "storage_type": "PostgreSQL",
                "confidence": 0.88,
                "reasoning": f"Небольшой объем данных ({total_size_mb}MB) - PostgreSQL для общих задач",
                "transformation_steps": ["Простая схема", "Базовые индексы"],
                "performance_estimate": "~10-15 секунд для обработки",
                "cost_estimate": "Очень низкая"
            }
    
    def enhance_ddl(self, base_ddl: str, context: Dict[str, Any]) -> str:
        """
        Улучшение DDL с помощью DeepSeek AI
        """
        
        messages = [
            {
                "role": "system",
                "content": "Ты - эксперт по SQL и оптимизации баз данных. Улучшай DDL скрипты, добавляя индексы, констрейнты и оптимизации."
            },
            {
                "role": "user",
                "content": f"Оптимизируй этот DDL скрипт:\n\n{base_ddl}\n\nКонтекст: {json.dumps(context, ensure_ascii=False, indent=2)}\n\nДобавь индексы, констрейнты и оптимизации. Верни только SQL код."
            }
        ]
        
        ai_response = self._call_deepseek_api(messages)
        
        # Если AI вернул корректный ответ, используем его
        if ai_response and "CREATE" in ai_response.upper() and len(ai_response) > len(base_ddl):
            return ai_response
        
        # Fallback логика
        enhanced_ddl = base_ddl
        
        # Автоматические улучшения
        if "id" in base_ddl.lower() and "index" not in base_ddl.lower():
            enhanced_ddl += "\n\n-- AI рекомендация: индекс на ID\nCREATE INDEX IF NOT EXISTS idx_primary_id ON target_table(id);"
        
        if "created_at" in base_ddl.lower():
            enhanced_ddl += "\n-- AI рекомендация: индекс на дату\nCREATE INDEX IF NOT EXISTS idx_created_at ON target_table(created_at);"
        
        enhanced_ddl += "\n\n-- Оптимизировано AI Data Engineer"
        
        return enhanced_ddl
    
    def generate_transformation_logic(self, 
                                    source_schema: Dict[str, Any], 
                                    target_schema: Dict[str, Any]) -> str:
        """
        Генерация логики трансформации данных через DeepSeek API
        """
        
        messages = [
            {
                "role": "system",
                "content": "Ты - эксперт по ETL и трансформациям данных. Генерируй Python/Pandas код для трансформации данных между схемами."
            },
            {
                "role": "user",
                "content": f"Сгенерируй Python/Pandas код для трансформации данных:\n\nИСХОДНАЯ СХЕМА:\n{json.dumps(source_schema, ensure_ascii=False, indent=2)}\n\nЦЕЛЕВАЯ СХЕМА:\n{json.dumps(target_schema, ensure_ascii=False, indent=2)}\n\nВерни готовый Python код для трансформации pandas DataFrame."
            }
        ]
        
        ai_response = self._call_deepseek_api(messages)
        
        # Очищаем ответ от markdown разметки
        if ai_response.startswith("```python"):
            ai_response = ai_response.replace("```python", "").replace("```", "").strip()
        
        # Если AI вернул корректный код
        if ai_response and ("df" in ai_response or "DataFrame" in ai_response):
            return f"# AI-генерированная трансформация\nimport pandas as pd\nfrom datetime import datetime\n\n{ai_response}"
        
        # Fallback логика
        transformations = [
            "# AI-генерированная трансформация",
            "import pandas as pd",
            "from datetime import datetime",
            "",
            "# Очистка данных",
            "df = df.dropna()  # Удаление пустых значений",
            "df = df.drop_duplicates()  # Удаление дубликатов",
            "",
            "# Нормализация строковых полей",
            "for col in df.select_dtypes(include=['object']).columns:",
            "    df[col] = df[col].astype(str).str.strip().str.lower()",
            "",
            "# Автоматическое определение типов",
            "df = df.infer_objects()",
            "",
            "# Добавление метаданных",
            "df['created_at'] = datetime.now()",
            "df['data_source'] = 'ai_migration'",
            "df['processing_version'] = '1.0'"
        ]
        
        return "\n".join(transformations)

# Для обратной совместимости
def create_llm_service() -> LLMService:
    """Создает экземпляр LLM сервиса"""
    return LLMService()