# Список Сервисов

## Очереди и брокеры
`redis-airflow` - брокер сообщений для Celery в Airflow
`kafka-broker` - кластер Kafka для потоковой обработки данных

## Оркестрация пайплайнов
`airflow-init` - инициализация Airflow
`airflow-webserver` - веб апи Airflow
`airflow-scheduler` - планировщик выполнения DAG файлов
`airflow-worker` - воркеры для выполнения задач
`airflow-dag-processor` - обработчик DAG файлов
`airflow-flower` - мониторинг Celery

## Хранилище данных
`hadoop-namenode` - управляющий узел HDFS
`hadoop-datanode` - узлы хранения данных HDFS

## Обработка данных
`spark-master` - главный узел Spark кластера
`spark-worker` - рабочие узлы Spark для распределенной обработки

## Reverse Proxy
`nginx` - точка входа для внешних запросов
`gateway` - yarp gateway для запросов к распределенным сервисам backend

## Web Client
`blazor-ssr` - веб клиент приложения

## backend
`internal` - веб апи для внутренних действий
`data-processing` - обработка источников/таргетов
`notification` - отправка нотификаций данных по web socket
`ai` - {добавить описание ai сервисов}


# Описание работы Data Processing сервиса
## Создание пайплайна
После указания пользователем источника/таргета проверяем доступность соединения в случае, если это подключение к чему либо, например база данных

После успешного подключения/наличия файла начинаем читать схему источника/таргета. 
Для файлов происходит передача на backend, где он загружается в hadoop для дальнейшей распределенной обработке с помощью spark и хранения

Как только получаем схему отправляем ее в kafka, topik - internal-scheme-ready и notification-scheme-ready, в Internal сохраняем данные в postgres, в notification отправляем клиенту уведомление о том, что схема готова и ее можно выгрузить

## Генерация пайплайнов с помощью ai 

Выгружаем собранные данные по пайплайну 
Следом подготавливаем запрос в ai и отправляем его в kafka, топик: ai-pipeline-request

Пример запроса

```json
{
    "request_uid": "{uid}",
    "pipeline_uid": "{uid}",
    "timestamp": "{current_date}",
    "sources": [
        {
            "type": "File",
            "parameters": {
                "type": "CSV",
                "filepath": "C:/file/path.csv",
                "delimeter": ";"
            },
            "schema-infos": [
                {
                    "size_mb": 123,
                    "row_count": 321545,
                    "fields": [
                        {
                            "name": "id",
                            "data_type": "Integer",
                            "filter": {
                                "op": "ge",
                                "value": 10
                            },
                            "sort": "DESC",
                            "nullable": false,
                            "sample_values": [
                                "1",
                                "2",
                                "3",
                                "4",
                                "5"
                            ],
                            "unique_values": 321545,
                            "null_count": 0,
                            "statistics": {
                                "min": "1",
                                "max": "1500000",
                                "avg": "750000.5"
                            }
                        },
                        {
                            "name": "description",
                            "data_type": "String",
                            "filter": null,
                            "sort": null,
                            "nullable": false,
                            "sample_values": [
                                "Vauxhall Corsa 120hp Turbo Design",
                                "Vauxhall Mokka Electric 100kwt Yes",
                                "Vauxhall Corsa 120hp Turbo Design",
                                "Vauxhall Mokka Electric 100kwt Yes",
                                "Vauxhall Corsa 120hp Turbo Design"
                            ],
                            "unique_values": 6666,
                            "null_count": 0,
                            "statistics": {
                                "min_length": 9,
                                "max_length": 15,
                                "avg_length": 12
                            }
                        }
                    ]
                }
            ]
        },
        {
            "type": "File",
            "parameters": {
                "type": "JSON",
                "filepath": "C:/file/path.json"
            },
            "schema_infos": [
                {
                    "size_mb": 123,
                    "row_count": 321545,
                    "fields": [
                        {
                            "name": "id",
                            "data_type": "Integer",
                            "filter": {
                                "op": "ge",
                                "value": 10
                            },
                            "sort": "DESC",
                            "nullable": false,
                            "sample_values": [
                                "1",
                                "2",
                                "3",
                                "4",
                                "5"
                            ],
                            "unique_values": 321545,
                            "null_count": 0,
                            "statistics": {
                                "min": "1",
                                "max": "1500000",
                                "avg": "750000.5"
                            }
                        },
                        {
                            "name": "description",
                            "data_type": "String",
                            "filter": null,
                            "sort": null,
                            "nullable": false,
                            "sample_values": [
                                "Vauxhall Corsa 120hp Turbo Design",
                                "Vauxhall Mokka Electric 100kwt Yes",
                                "Vauxhall Corsa 120hp Turbo Design",
                                "Vauxhall Mokka Electric 100kwt Yes",
                                "Vauxhall Corsa 120hp Turbo Design"
                            ],
                            "unique_values": 6666,
                            "null_count": 0,
                            "statistics": {
                                "min_length": 9,
                                "max_length": 15,
                                "avg_length": 12
                            }
                        },
                        {
                            "name": "user",
                            "fields": [
                                {
                                    "name": "name",
                                    "data_type": "String",
                                    "filter": null,
                                    "sort": null,
                                    "nullable": false,
                                    "sample_values": [
                                        "Александр",
                                        "Елена",
                                        "Олег",
                                        "Максим",
                                        "Юлия"
                                    ],
                                    "unique_values": 6666,
                                    "null_count": 0,
                                    "statistics": {
                                        "min_length": 9,
                                        "max_length": 15,
                                        "avg_length": 12
                                    }
                                }
                            ]
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
                "database": "postgre",
                "user": "admin",
                "password": "123"
            },
            "schema-infos": [
                {
                    "size_mb": 123,
                    "row_count": 321545,
                    "tables": [
                        {
                            "name": "Vehicles",
                            "columns": [
                                {
                                    "name": "id",
                                    "data_type": "Integer",
                                    "filter": null,
                                    "sort": null,
                                    "nullable": false,
                                    "is_primary": true,
                                    "sample_values": [
                                        "1",
                                        "2",
                                        "3",
                                        "4",
                                        "5"
                                    ],
                                    "unique_values": 321545,
                                    "null_count": 0,
                                    "statistics": {
                                        "min": "1",
                                        "max": "1500000",
                                        "avg": "750000.5"
                                    }
                                },
                                {
                                    "name": "description",
                                    "data_type": "String",
                                    "filter": null,
                                    "sort": null,
                                    "nullable": false,
                                    "is_primary": false,
                                    "sample_values": [
                                        "Vauxhall Corsa 120hp Turbo Design",
                                        "Vauxhall Mokka Electric 100kwt Yes",
                                        "Vauxhall Corsa 120hp Turbo Design",
                                        "Vauxhall Mokka Electric 100kwt Yes",
                                        "Vauxhall Corsa 120hp Turbo Design"
                                    ],
                                    "unique_values": 6666,
                                    "null_count": 0,
                                    "statistics": {
                                        "min_length": 9,
                                        "max_length": 15,
                                        "avg_length": 12
                                    }
                                }
                            ],
                            "foreign_keys": [
                                {
                                    "name": "FK_Vehicles_Users_Id",
                                    "primary_table": "Users",
                                    "primary_column": "Id",
                                    "foreign_table": "Vehicles",
                                    "foreign_column": "UserId"
                                }
                            ],
                            "indexes": [
                                {
                                    "name": "IX_Unique_UserId",
                                    "is_unique": true,
                                    "is_clustered": false,
                                    "columns": ["UserId"]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}
```

В `sources` свойства `filter` и `sort`, при первом запросе, всегда null
В `targets` свойства `filter` и `sort` всегда null


## Получение ответа от ai

Прослушиваем топик в kafka: ai-pipeline-response
При получении данных начинаем сохранение данных:
1) Сохраняем DAG файл и регистрируем его airflow
2) Сохраняем данные по пайплайну в бд
3) При наличии рекомендации в периодическом запуске - регистрируем в airflow scheduler

После отправки всех событий отправляем в kafka, topic - notification-ready-ai-response, где сервис уведомлений отправит на клиент уведомление о том, что генерация пайплайна завершена

## Запуск пайплайна

Начинаем процесс чтения схем источников
Если схема не изменилась, то отправляем в airflow запрос на запуск DAG соответствующий пайплайну
Если схема в каком либо источнике изменилась, то отвечаем предупреждением пользователю, что необходимо перегенерировать пайплайн

## Добавление Фильтра и Сортировки
Пользователь может добавить фильтр и/или сортировку, по 1 на каждую колонку/поле
После внесения пользователем изменений начинаем процесс генерации пайплайна

## Редактирование пайплайна

Вариант 1 - предоставляем пользователю на редактирование функцию, которая составляет пайплайн, после чего пересобираем DAG файл

Вариант 2 - пользователь текстом пишет что хочет изменить, после чего начинаем перегенерацию пайплана с учетом указанного пользователем