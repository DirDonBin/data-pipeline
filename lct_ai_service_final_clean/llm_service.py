"""
LLM Service для работы с DeepSeek API
Исправленная версия с поддержкой unlimited tokens
"""
import json
import logging
import httpx
from typing import Dict, Any, List, Optional
from config_loader import get_config

logger = logging.getLogger(__name__)

class LLMService:
    """Сервис для работы с LLM API"""
    
    def __init__(self):
        self.api_key = get_config('deepseek.api_key')
        self.base_url = get_config('deepseek.base_url')
        self.model = get_config('deepseek.model')
        self.max_tokens = get_config('deepseek.max_tokens')  # Может быть None для unlimited
        self.temperature = get_config('deepseek.temperature', 0.3)
        self.timeout = get_config('deepseek.timeout', 60)
        
        if not self.api_key:
            raise ValueError("DeepSeek API key не найден в конфигурации")
        
        logger.info(f"LLM Service инициализирован с {self.base_url}, модель: {self.model}")
    
    async def _call_deepseek_api(self, prompt: str) -> Dict[str, Any]:
        """Вызов DeepSeek API"""
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Формируем payload - если max_tokens None, не включаем это поле
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": self.temperature
        }
        
        # ИСПРАВЛЕНИЕ: добавляем max_tokens только если он не None
        if self.max_tokens is not None:
            payload["max_tokens"] = self.max_tokens
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                
                result = response.json()
                return {
                    "success": True,
                    "content": result["choices"][0]["message"]["content"],
                    "usage": result.get("usage", {})
                }
        
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP {e.response.status_code}: {e.response.text}"
            logger.error(f"HTTP ошибка DeepSeek API: {error_msg}")
            return {"success": False, "error": error_msg}
        except httpx.TimeoutException as e:
            error_msg = f"Timeout после {self.timeout}s: {str(e)}"
            logger.error(f"Timeout DeepSeek API: {error_msg}")
            return {"success": False, "error": error_msg}
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e) if str(e) else 'Unknown error'}"
            logger.error(f"Ошибка вызова DeepSeek API: {error_msg}")
            logger.error(f"Exception details: {repr(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {"success": False, "error": error_msg}
    
    def get_storage_recommendation(self, sources: List[Dict], targets: List[Dict], business_context: str) -> Dict[str, Any]:
        """Синхронная обертка для получения рекомендаций по хранилищу"""
        import asyncio
        
        try:
            # Создаем новый event loop если его нет
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Запускаем асинхронную функцию
            return loop.run_until_complete(self._get_storage_recommendation_async(sources, targets, business_context))
        
        except Exception as e:
            logger.error(f"Ошибка получения AI рекомендации: {e}")
            return {
                "storage_type": "PostgreSQL",
                "confidence": 0.75,
                "reasoning": f"Fallback рекомендация из-за ошибки AI: {str(e)}",
                "transformation_steps": ["Базовая миграция", "Проверка данных"],
                "performance_estimate": "Средняя производительность",
                "cost_estimate": "Средняя стоимость"
            }
    
    async def _get_storage_recommendation_async(self, sources: List[Dict], targets: List[Dict], business_context: str) -> Dict[str, Any]:
        """Получение рекомендации по выбору хранилища данных"""
        
        # Формируем детальный prompt
        prompt = f"""Проанализируй данные источники и цели для миграции данных.

Источники: {json.dumps(sources, ensure_ascii=False, indent=2)}
Цели: {json.dumps(targets, ensure_ascii=False, indent=2)}
Бизнес-контекст: {business_context}

Дай рекомендацию по выбору системы хранения данных. Учти:
1. Объем данных и структуру
2. Целевую систему  
3. Требования к производительности
4. Сложность трансформации

Ответь СТРОГО в JSON формате со следующими полями:
{{
  "storage_type": "тип хранилища",
  "confidence": число от 0 до 1,
  "reasoning": "подробное обоснование",
  "transformation_steps": ["список шагов трансформации"],
  "performance_estimate": "оценка производительности",
  "cost_estimate": "оценка стоимости"
}}"""

        try:
            # Вызываем LLM API
            result = await self._call_deepseek_api(prompt)
            
            if not result["success"]:
                raise Exception(result["error"])
            
            # Парсим JSON ответ
            content = result["content"].strip()
            
            # Извлекаем JSON из ответа (может быть обернут в ```json)
            if "```json" in content:
                json_start = content.find("```json") + 7
                json_end = content.find("```", json_start)
                content = content[json_start:json_end].strip()
            elif content.startswith("```") and content.endswith("```"):
                content = content[3:-3].strip()
            
            recommendation = json.loads(content)
            
            # Валидация обязательных полей
            required_fields = ["storage_type", "confidence", "reasoning"]
            for field in required_fields:
                if field not in recommendation:
                    raise ValueError(f"Отсутствует обязательное поле: {field}")
            
            logger.info(f"AI рекомендация получена: {recommendation['storage_type']} (уверенность: {recommendation['confidence']:.0%})")
            return recommendation
            
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON ответа от LLM: {e}")
            logger.error(f"Контент ответа: {content}")
            raise Exception(f"Некорректный JSON ответ от AI")
        
        except Exception as e:
            logger.error(f"Ошибка получения AI рекомендации: {e}")
            raise
    
    def generate_pipeline_description(self, prompt: str) -> str:
        """Синхронная обертка для генерации описания пайплайна"""
        import asyncio
        import concurrent.futures
        import threading
        
        def run_async_in_thread():
            """Запуск асинхронной функции в новом потоке с новым event loop"""
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                return new_loop.run_until_complete(self._call_deepseek_api(prompt))
            finally:
                new_loop.close()
        
        try:
            # Проверяем, запущен ли уже event loop
            try:
                asyncio.get_running_loop()
                # Если event loop уже запущен (например, в FastAPI), 
                # запускаем в отдельном потоке
                logger.info("Обнаружен запущенный event loop, используем ThreadPoolExecutor")
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_async_in_thread)
                    result = future.result(timeout=120)  # Увеличиваем timeout до 120 секунд
                    logger.info(f"Результат из потока получен: success={result.get('success', False)}")
            except RuntimeError:
                # Если event loop не запущен, запускаем напрямую
                logger.info("Event loop не запущен, используем asyncio.run")
                result = asyncio.run(self._call_deepseek_api(prompt))
                logger.info(f"Результат из asyncio.run получен: success={result.get('success', False)}")
            
            if result.get("success", False):
                content = result.get("content", "")
                logger.info(f"LLM описание пайплайна успешно сгенерировано, длина: {len(content)} символов")
                return content
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Ошибка LLM API: {error_msg}")
                logger.error(f"Полный результат: {result}")
                return f"Ошибка генерации описания: {error_msg}"
        
        except concurrent.futures.TimeoutError:
            logger.error("Timeout при выполнении LLM запроса (превышено 120 секунд)")
            return """
ОТЧЕТ О МИГРАЦИИ ДАННЫХ
=======================

Создан пайплайн для автоматизированной миграции данных между системами.
LLM описание недоступно из-за превышения времени ожидания ответа.

ОСНОВНЫЕ ЭТАПЫ:
1. Анализ источников данных и целевых систем
2. Создание схемы в целевой системе  
3. Извлечение данных из источников
4. Трансформация и очистка данных
5. Загрузка в целевую систему
6. Валидация корректности миграции
7. Генерация итогового отчета

ТЕХНИЧЕСКИЕ ХАРАКТЕРИСТИКИ:
- Поддержка множественных источников данных
- Автоматическая обработка ошибок и retry логика
- Мониторинг процесса миграции в реальном времени
- Детальное логирование всех операций
- Откат изменений в случае критических ошибок

Пайплайн готов к запуску и мониторингу.
"""
        
        except Exception as e:
            logger.error(f"Ошибка генерации описания пайплайна: {e}")
            logger.error(f"Тип ошибки: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Возвращаем детальное fallback описание
            return f"""
ОТЧЕТ О МИГРАЦИИ ДАННЫХ
=======================

Создан пайплайн для автоматизированной миграции данных между системами.
Детали недоступны из-за ошибки LLM API: {str(e)}

ОСНОВНЫЕ ЭТАПЫ:
1. Анализ источников данных и целевых систем
2. Создание схемы в целевой системе  
3. Извлечение данных из источников
4. Трансформация и очистка данных
5. Загрузка в целевую систему
6. Валидация корректности миграции
7. Генерация итогового отчета

ТЕХНИЧЕСКИЕ ХАРАКТЕРИСТИКИ:
- Поддержка множественных источников данных
- Автоматическая обработка ошибок и retry логика
- Мониторинг процесса миграции в реальном времени
- Детальное логирование всех операций
- Откат изменений в случае критических ошибок

Пайплайн готов к запуску и мониторингу.
"""
