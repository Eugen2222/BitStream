import json
from kafka import KafkaProducer
from websocket import WebSocketApp

KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "market-trade-topic"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def on_open(ws):
    print("Connected to Binance trade stream")


def on_message(ws, message):
    data = json.loads(message)

    payload = {
        "trade_id": int(data["t"]),
        "symbol": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "event_time_ms": int(data["E"]),
        "trade_time_ms": int(data["T"])
    }

    producer.send(KAFKA_TOPIC, payload)
    print("sent:", payload)


def on_error(ws, error):
    print("websocket error:", error)


def on_close(ws, close_status_code, close_msg):
    print("websocket closed:", close_status_code, close_msg)


if __name__ == "__main__":
    ws = WebSocketApp(
        "wss://data-stream.binance.vision/ws/btcusdt@trade",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()