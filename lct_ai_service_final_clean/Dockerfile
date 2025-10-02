# LCT AI Service - Обновленный Docker Image 
# Поддержка новых требований команды ЛЦТ: Hadoop + Spark + новый формат

FROM python:3.11-slim

# Устанавливаем системные зависимости включая Java для Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    wget \
    default-jdk \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Настраиваем Java для Spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Создаем рабочую директорию
WORKDIR /app

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем дополнительные зависимости для Hadoop и Spark
RUN pip install --no-cache-dir \
    pyspark==3.4.0 \
    py4j==0.10.9.7 \
    hdfs3==0.3.1 \
    kafka-python==2.0.2

# Копируем исходный код приложения
COPY *.py ./

COPY config.json ./

# Создаем директории для данных, логов и Hadoop/Spark
RUN mkdir -p /app/data /app/logs /app/output /app/hadoop_data /app/spark_data

# Создаем пользователя для безопасности
RUN groupadd -r lctuser && useradd -r -g lctuser lctuser
RUN chown -R lctuser:lctuser /app
USER lctuser

# Переменные окружения
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=/app/hadoop_conf

# Открываем порты (HTTP API + Spark UI + Kafka)
EXPOSE 8080 4040 9092

# Healthcheck для проверки работоспособности
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import updated_lct_service; print('OK')" || exit 1

# Команда по умолчанию - запуск HTTP API сервиса
CMD ["python", "main_service.py"]