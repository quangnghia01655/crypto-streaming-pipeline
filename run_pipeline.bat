@echo off
echo ===================================================
echo  Starting Crypto Streaming Pipeline
echo ===================================================

echo [1/4] Starting Docker infrastructure (Kafka & Spark)...
docker-compose up -d

echo Waiting 15 seconds for Kafka to fully initialize...
timeout /t 15 /nobreak >nul

echo [2/4] Starting Python Binance Producer...
start "Binance Producer" cmd /k "python src/producer.py"

echo [3/4] Starting PySpark Stream-to-Stream Join...
start "PySpark Processor" cmd /k "docker-compose exec spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/src/spark_processor.py"

echo [4/4] Starting Streamlit Real-Time Dashboard...
start "Streamlit Dashboard" cmd /k "streamlit run dashboard.py"

echo ===================================================
echo  All services launched in separate windows!
echo ===================================================
pause