"""Consume and display the alerts produced by Spark Structured Streaming."""

import json
import signal
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from config import BOOTSTRAP_SERVERS, GENERAL_ALERTS_TOPIC, TOPIC_PREFIX


running = True


def stop(_signum: int, _frame: Any) -> None:
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"{TOPIC_PREFIX}-spark-alerts-display",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([GENERAL_ALERTS_TOPIC])
    print(f"Waiting for Spark alerts from {GENERAL_ALERTS_TOPIC}", flush=True)

    try:
        while running:
            kafka_message = consumer.poll(1.0)
            if kafka_message is None:
                continue
            if kafka_message.error():
                if kafka_message.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(kafka_message.error())
                continue

            try:
                alert = json.loads(kafka_message.value().decode())
                print(
                    f"[ALERT {alert['code']}] {alert['message']} | "
                    f"window={alert['window_start']} .. {alert['window_end']} | "
                    f"temperature_avg={alert['temperature_avg']:.2f}°C | "
                    f"humidity_avg={alert['humidity_avg']:.2f}% | "
                    f"partition={kafka_message.partition()} "
                    f"offset={kafka_message.offset()}",
                    flush=True,
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                print(
                    f"Skipped invalid alert: {error}; "
                    f"value={kafka_message.value()!r}",
                    flush=True,
                )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
