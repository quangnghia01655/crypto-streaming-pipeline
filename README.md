# Real-Time Crypto Arbitrage Streaming Pipeline 

An end-to-end, multi-stream Data Engineering pipeline that ingests, buffers, joins, and visualizes high-velocity cryptocurrency trades in real-time. This system consumes multiplexed WebSocket feeds from Binance, buffers the data via Apache Kafka, performs complex Stream-to-Stream joins using PySpark, sinks to a Parquet Data Lake, and serves insights via a live Streamlit dashboard.

##  Architecture

1. **Ingestion Layer (Python):** A WebSocket client connects to the Binance multiplex API, simultaneously extracting live BTC/USDT and ETH/USDT trade events and routing them to dedicated Kafka topics.
2. **Message Broker (Kafka):** Confluent Apache Kafka (running in Docker via KRaft) buffers the raw JSON streams, ensuring fault tolerance and zero data loss during high-volume market spikes.
3. **Processing Layer (PySpark):** PySpark Structured Streaming consumes both Kafka topics. It applies a 1-minute event-time watermark to handle late-arriving data and executes a **Stream-to-Stream Join** with an equality predicate to match BTC and ETH trades occurring within a 10-second window, calculating the real-time price ratio.
4. **Storage Sink (Parquet Data Lake):** Processed micro-batches are appended to a local Data Lake in columnar `.parquet` format for efficient downstream querying.
5. **Serving Layer (Streamlit):** A live web dashboard continuously polls the Data Lake, rendering the latest prices, ratios, and moving trend lines in real-time.

## Tech Stack
* **Languages:** Python 3, SQL
* **Streaming & Processing:** Apache Kafka, PySpark (Structured Streaming)
* **Visualization:** Streamlit, Pandas
* **Infrastructure:** Docker, Docker Compose, Windows Batch Automation
* **Storage:** Parquet, Local File System

## Project Structure
```text
crypto-streaming-pipeline/
├── src/
│   ├── producer.py           # Multi-stream Binance WebSocket -> Kafka
│   └── spark_processor.py    # PySpark stream-to-stream join & Parquet sink
├── data/
│   ├── checkpoints_ratio/    # Spark streaming state management
│   └── output/               # Parquet Data Lake (crypto_ratio/)
├── docker-compose.yml        # Infrastructure setup (Kafka broker & Spark cluster)
├── dashboard.py              # Real-time Streamlit visualization app
├── requirements.txt          # Python dependencies
├── run_pipeline.bat          # 1-click startup automation script
└── stop_pipeline.bat         # Teardown and cleanup script
```

## How to Run the Pipeline

### Prerequisites
* Docker Desktop installed and running
* Python 3.8+ installed
* Install required Python libraries: `pip install -r requirements.txt`

### 1-Click Deployment
This project includes Windows Batch scripts to automate the infrastructure orchestration and script execution.

1. **Start the Pipeline:** Double-click `run_pipeline.bat`. 
   * This automatically spins up the Kafka and Spark Docker containers, waits for initialization, and launches the Producer, Spark Processor, and Streamlit Dashboard in dedicated terminal windows.
2. **View the Dashboard:** A browser window will automatically open to `http://localhost:8501` to display the live charts.
3. **Stop the Pipeline:** Double-click `stop_pipeline.bat` to spin down the Docker containers and terminate all background processes.