import json
import websocket
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(ws, message):
    raw_data = json.loads(message)
    
    # Binance combined streams wrap data in {"stream": "...", "data": {...}}
    stream_name = raw_data.get('stream', '')
    trade_data = raw_data.get('data', {})
    
    payload = {
        'price': float(trade_data.get('p', 0)),
        'quantity': float(trade_data.get('q', 0)),
        'trade_time_ms': trade_data.get('E')
    }
    
    # Route to the correct Kafka topic based on the stream name
    if 'btc' in stream_name:
        producer.send('btc_trades', payload)
        print(f"BTC Trade: ${payload['price']}")
    elif 'eth' in stream_name:
        producer.send('eth_trades', payload)
        print(f"ETH Trade: ${payload['price']}")

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

if __name__ == "__main__":
    print("Starting Multi-Stream: Fetching BTC and ETH trades...")
    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade",
        on_message=on_message,
        on_error=on_error
    )
    ws.run_forever()