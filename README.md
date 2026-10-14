# Real-Time BTC/ETH Ratio Streaming Pipeline

An end-to-end streaming data pipeline that ingests live Binance trades for BTC/USDT and ETH/USDT, buffers them in Kafka, aggregates them per second with PySpark Structured Streaming, stores the result as Parquet, and serves a live Streamlit dashboard.

## Architecture

```text
Binance WebSocket -> Python producer -> Kafka (btc_trades, eth_trades)
    -> PySpark Structured Streaming (1-second event-time windows)
    -> Parquet files -> Streamlit dashboard
```

1. **Ingestion (Python):** A WebSocket client reads Binance's combined stream and publishes each trade to a dedicated Kafka topic (`btc_trades` or `eth_trades`).
2. **Message broker (Kafka, KRaft, Docker):** Decouples ingestion from processing. Messages are retained on the broker, so Spark can restart and resume from its checkpoint.
3. **Processing (PySpark):** Reads both topics, parses and validates the JSON (malformed records are dropped), converts the trade timestamp to event time, and applies a 10-second watermark. A single 1-second tumbling-window aggregation computes the average BTC and ETH price per second and the BTC/ETH ratio. Only seconds where both symbols traded are kept.
4. **Storage (Parquet):** Micro-batches (every 10 seconds) are appended to a local Parquet data lake. Spark checkpointing tracks offsets and state.
5. **Serving (Streamlit):** The dashboard reads the latest rows and refreshes every 2 seconds.

## Design decisions

- **Windowed aggregation instead of a stream-to-stream join.** The first version joined raw BTC and ETH trades within +/-10 seconds on a constant key. That matched every BTC trade with every ETH trade in the window, producing far more rows than either input. Aggregating to one row per second per symbol gives a clean one-row-per-second result.
- **Event time, not arrival time.** The producer forwards Binance's event timestamp, and Spark windows on it, so late or out-of-order trades land in the right window.
- **Millisecond precision.** Timestamps are derived by dividing epoch milliseconds by 1000 and casting, instead of `from_unixtime`, which truncates to seconds.

## Limitations

- Single-node Kafka with replication factor 1, and Spark in `local[*]` mode: a demo setup, not fault tolerant.
- The producer has no reconnect logic and uses default delivery settings (at-least-once at best).
- A 10-second trigger creates many small Parquet files, and the dashboard re-reads the directory on each refresh. A table format such as Delta Lake or periodic compaction would fix this.
- No automated tests or monitoring (for example consumer lag).

## Tech stack

Python 3, Apache Kafka, PySpark (Structured Streaming), Parquet, Streamlit, Pandas, Docker / Docker Compose, Windows batch scripts.

## Project structure

```text
crypto-streaming-pipeline/
├── src/
│   ├── producer.py           # Binance WebSocket -> Kafka
│   └── spark_processor.py    # Windowed aggregation & Parquet sink
├── data/
│   ├── checkpoints_ratio/    # Spark streaming state
│   └── output/               # Parquet data lake (crypto_ratio/)
├── docker-compose.yml        # Kafka broker and Spark container
├── dashboard.py              # Streamlit app
├── requirements.txt
├── run_pipeline.bat          # One-click startup
└── stop_pipeline.bat         # Teardown
```

## How to run

**Prerequisites:** Docker Desktop, Python 3.8+, `pip install -r requirements.txt`.

1. Double-click `run_pipeline.bat`. It starts the Kafka and Spark containers, then launches the producer, the Spark job, and the dashboard.
2. Open `http://localhost:8501`. The first rows appear roughly 15-25 seconds after startup (10-second watermark plus the 10-second trigger).
3. Double-click `stop_pipeline.bat` to shut everything down.

> If you change the Spark query, delete `data/checkpoints_ratio/` and `data/output/crypto_ratio/` before restarting, because an old checkpoint is incompatible with a changed query.
