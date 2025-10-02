"""
Data Migration Analyzer - агент для анализа данных и рекомендаций по миграции
Интегрируется с существующим LLM сервисом
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class DataMigrationAnalyzer:
    """Анализирует данные и рекомендует оптимальные целевые системы"""
    
    def __init__(self, llm_service):
        self.llm_service = llm_service
        self.storage_rules = self._load_storage_rules()
    
    def _load_storage_rules(self) -> Dict[str, Any]:
        """Правила выбора систем хранения"""
        return {
            "clickhouse": {
                "conditions": ["analytics", "aggregated", "time_series", "large_volume"],
                "data_types": ["numeric", "datetime"],
                "min_size_mb": 100,
                "use_cases": ["аналитика", "отчеты", "метрики", "временные ряды"]
            },
            "postgresql": {
                "conditions": ["transactional", "operational", "relational"],
                "data_types": ["mixed", "string", "json"],
                "max_size_mb": 1000,
                "use_cases": ["OLTP", "операционные данные", "пользователи", "каталоги"]
            },
            "hdfs": {
                "conditions": ["raw_data", "backup", "large_files"],
                "min_size_mb": 500,
                "file_formats": ["parquet", "avro", "csv", "json"],
                "use_cases": ["сырые данные", "архив", "data lake"]
            },
            "kafka": {
                "conditions": ["streaming", "real_time", "events"],
                "data_types": ["events", "logs", "messages"],
                "use_cases": ["потоковые данные", "события", "логи", "real-time"]
            }
        }
    
    def analyze_data_characteristics(self, sources: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Анализирует характеристики данных"""
        analysis = {
            "total_size_mb": 0,
            "total_records": 0,
            "data_types": set(),
            "file_formats": set(),
            "has_time_columns": False,
            "has_numeric_data": False,
            "complexity": "simple"
        }
        
        for source in sources:
            # Поддержка нового формата fix.md
            if "estimated_rows" in source and "file_size_mb" in source:
                # Новый формат - используем напрямую
                size_mb = source.get("file_size_mb", 0)
                records = source.get("estimated_rows", 0)
                source_format = source.get("format", "unknown")
                characteristics = source.get("data_characteristics", {})
            else:
                # Старый формат - через parameters
                params = source.get("parameters", {})
                size_mb = params.get("file_size_mb", 0)
                records = params.get("row_count", 0)
                source_format = source.get("$type", "unknown")
                characteristics = {}
            
            # Обновляем общую статистику
            analysis["total_size_mb"] += size_mb
            analysis["total_records"] += records
            
            # Форматы файлов
            analysis["file_formats"].add(source_format)
            
            # Анализ характеристик данных (новый формат)
            if characteristics:
                volume = characteristics.get("volume", "")
                velocity = characteristics.get("velocity", "")
                variety = characteristics.get("variety", "")
                complexity = characteristics.get("complexity", "")
                
                # Определяем наличие временных колонок из velocity
                if velocity in ["realtime", "high"]:
                    analysis["has_time_columns"] = True
                
                # Определяем числовые данные из variety
                if variety in ["structured"]:
                    analysis["has_numeric_data"] = True
                
                # Обновляем сложность на основе характеристик
                if complexity == "high" or volume == "very_high":
                    analysis["complexity"] = "complex"
                elif complexity == "medium" or volume == "high":
                    analysis["complexity"] = "medium"
            else:
                # Старый способ анализа колонок
                schema_info = source.get("schema_infos", [{}])[0]
                columns = schema_info.get("columns", [])
                for column in columns:
                    data_type = column.get("data_type", "").lower()
                    analysis["data_types"].add(data_type)
                    
                    if "time" in data_type or "date" in data_type:
                        analysis["has_time_columns"] = True
                    
                    if data_type in ["integer", "decimal", "float", "numeric"]:
                        analysis["has_numeric_data"] = True
        
        # Определение сложности
        if len(sources) > 3 or analysis["total_size_mb"] > 1000:
            analysis["complexity"] = "complex"
        elif analysis["total_size_mb"] > 100:
            analysis["complexity"] = "medium"
        
        return analysis
    
    def recommend_target_systems(self, analysis: Dict[str, Any], 
                                business_context: str = "") -> List[Dict[str, Any]]:
        """Рекомендует целевые системы на основе анализа"""
        recommendations = []
        
        # ClickHouse для аналитических данных
        if (analysis["has_time_columns"] and analysis["has_numeric_data"] and 
            analysis["total_size_mb"] > 50):
            recommendations.append({
                "system": "ClickHouse",
                "confidence": 0.9,
                "reasons": [
                    "Временные данные с числовыми метриками",
                    f"Объем данных {analysis['total_size_mb']}MB подходит для аналитики",
                    "Оптимизирован для OLAP запросов"
                ],
                "recommendations": [
                    "Партицирование по дате",
                    "Сжатие данных LZ4",
                    "Материализованные представления для агрегатов"
                ]
            })
        
        # PostgreSQL для операционных данных
        if (analysis["complexity"] == "simple" and 
            analysis["total_size_mb"] < 500 and
            "string" in analysis["data_types"]):
            recommendations.append({
                "system": "PostgreSQL",
                "confidence": 0.8,
                "reasons": [
                    "Небольшой объем данных для OLTP",
                    "Смешанные типы данных",
                    "Подходит для операционных задач"
                ],
                "recommendations": [
                    "Индексы на часто используемые поля",
                    "Внешние ключи для связности",
                    "Регулярное обслуживание VACUUM"
                ]
            })
        
        # HDFS для больших сырых данных
        if analysis["total_size_mb"] > 500 or "parquet" in analysis["file_formats"]:
            recommendations.append({
                "system": "HDFS",
                "confidence": 0.7,
                "reasons": [
                    f"Большой объем данных {analysis['total_size_mb']}MB",
                    "Подходит для хранения сырых данных",
                    "Масштабируемое хранилище"
                ],
                "recommendations": [
                    "Формат Parquet для эффективности",
                    "Партицирование по дате/категории",
                    "Сжатие Snappy"
                ]
            })
        
        # Сортировка по confidence
        recommendations.sort(key=lambda x: x["confidence"], reverse=True)
        
        return recommendations[:3]  # Топ-3 рекомендации
    
    async def generate_migration_analysis(self, sources: List[Dict[str, Any]], 
                                        targets: List[Dict[str, Any]] = None) -> str:
        """Генерирует полный анализ миграции с помощью LLM"""
        
        analysis = self.analyze_data_characteristics(sources)
        recommendations = self.recommend_target_systems(analysis)
        
        # Подготовка промпта для LLM
        prompt = f"""
Как эксперт по миграции данных, проанализируй следующую ситуацию:

ХАРАКТЕРИСТИКИ ДАННЫХ:
- Общий объем: {analysis['total_size_mb']}MB
- Количество записей: {analysis['total_records']}
- Типы данных: {', '.join(analysis['data_types'])}
- Форматы файлов: {', '.join(analysis['file_formats'])}
- Временные колонки: {'Да' if analysis['has_time_columns'] else 'Нет'}
- Числовые данные: {'Да' if analysis['has_numeric_data'] else 'Нет'}
- Сложность: {analysis['complexity']}

АВТОМАТИЧЕСКИЕ РЕКОМЕНДАЦИИ:
{self._format_recommendations(recommendations)}

ИСТОЧНИКИ ДАННЫХ:
{self._format_sources(sources)}

Предоставь детальный анализ и рекомендации:
1. Оптимальную архитектуру хранения
2. Обоснование выбора СУБД
3. Рекомендации по структуре данных
4. ETL стратегию
5. Потенциальные риски и их решения
"""

        try:
            # Генерация анализа через LLM
            llm_analysis = await self.llm_service.generate_pipeline_description(
                prompt, sources, targets or []
            )
            
            return llm_analysis
            
        except Exception as e:
            logger.error(f"Ошибка генерации анализа миграции: {e}")
            
            # Fallback - базовый анализ
            return self._generate_basic_analysis(analysis, recommendations)
    
    def _format_recommendations(self, recommendations: List[Dict[str, Any]]) -> str:
        """Форматирует рекомендации для промпта"""
        formatted = []
        for i, rec in enumerate(recommendations, 1):
            reasons = '\n  '.join(rec['reasons'])
            recs = '\n  '.join(rec['recommendations'])
            formatted.append(f"""
{i}. {rec['system']} (уверенность: {rec['confidence']:.0%})
  Причины:
  {reasons}
  Рекомендации:
  {recs}
""")
        return '\n'.join(formatted)
    
    def _format_sources(self, sources: List[Dict[str, Any]]) -> str:
        """Форматирует источники для промпта"""
        formatted = []
        for i, source in enumerate(sources, 1):
            params = source.get("parameters", {})
            formatted.append(f"""
{i}. {source.get('$type', 'unknown')}
  Путь: {params.get('file_path', 'N/A')}
  Размер: {params.get('file_size_mb', 'N/A')}MB
  Записи: {params.get('row_count', 'N/A')}
""")
        return '\n'.join(formatted)
    
    def _generate_basic_analysis(self, analysis: Dict[str, Any], 
                                recommendations: List[Dict[str, Any]]) -> str:
        """Генерирует базовый анализ без LLM"""
        
        report = f"""
# Анализ миграции данных

## Характеристики данных
- Объем: {analysis['total_size_mb']}MB
- Записи: {analysis['total_records']}
- Сложность: {analysis['complexity']}
- Временные данные: {'Да' if analysis['has_time_columns'] else 'Нет'}

## Рекомендации по хранению

"""
        
        for i, rec in enumerate(recommendations, 1):
            report += f"""
### {i}. {rec['system']} (уверенность: {rec['confidence']:.0%})

**Обоснование:**
{chr(10).join(f"- {reason}" for reason in rec['reasons'])}

**Рекомендации:**
{chr(10).join(f"- {r}" for r in rec['recommendations'])}
"""
        
        report += f"""

## Стратегия миграции
- Рекомендуется начать с {recommendations[0]['system']} как основного хранилища
- Использовать пошаговую миграцию с валидацией данных
- Настроить мониторинг производительности

Дата анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        return report
