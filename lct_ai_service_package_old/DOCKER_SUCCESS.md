# Docker Build & Deployment Guide

## ✅ Docker Image Successfully Built!

Образ `lct-ai-service:simple` успешно собран и протестирован.

## Quick Start

1. **Запуск контейнера:**
```bash
docker run -p 8080:8080 lct-ai-service:simple
```

2. **Сборка образа (если нужно пересобрать):**
```bash
docker build -f Dockerfile.simple -t lct-ai-service:simple .
```

## Production Deployment

1. **С docker-compose:**
```bash
docker-compose -f docker-compose-lct-integration.yml up
```

2. **С переменными окружения:**
```bash
docker run -p 8080:8080 --env-file .env.lct lct-ai-service:simple
```

## Image Info

- **Base Image:** python:3.11-slim
- **Size:** ~200MB (оптимизировано)
- **Dependencies:** Только Python пакеты, без системных компиляторов
- **Port:** 8080
- **Entry Point:** test_service.py

## Integration Status

✅ Dockerfile создан и протестирован
✅ Docker образ собирается без ошибок  
✅ Контейнер запускается успешно
✅ Готов к интеграции с вашей инфраструктурой

## Troubleshooting

- Если есть проблемы с правами доступа к Docker, используйте полный путь к docker.exe
- Для Windows: `&"C:\Program Files\Docker\Docker\resources\bin\docker.exe"`
- Для WSL/Linux: стандартные команды `docker`

## Next Steps

1. Скопируйте папку `lct_ai_service_package` в вашу инфраструктуру
2. Используйте `docker-compose-lct-integration.yml` для интеграции с Kafka
3. Адаптируйте `.env.lct` под ваши настройки