"""Simulate one IoT sensor and continuously publish its readings to Kafka."""

import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import KafkaError, Producer

from config import BOOTSTRAP_SERVERS, BUILDING_SENSORS_TOPIC


running = True


def stop(_signum: int, _frame: Any) -> None:
    """Request a graceful shutdown after the current producer iteration."""
    global running
    running = False


def env_float(name: str, default: float) -> float:
    """Read one floating-point setting from the environment."""
    return float(os.getenv(name, str(default)))


def make_reading(
    sensor_id: str,
    temperature_range: tuple[float, float],
    humidity_range: tuple[float, float],
) -> dict[str, object]:
    """Build one sensor event in the schema consumed by Spark."""
    return {
        "id": sensor_id,
        "temperature": round(random.uniform(*temperature_range), 2),
        "humidity": round(random.uniform(*humidity_range), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(error: KafkaError | None, message: Any) -> None:
    """Log Kafka delivery confirmation or failure."""
    if error is not None:
        print(f"Delivery failed: {error}", flush=True)
        return
    print(
        f"Delivered to {message.topic()} "
        f"partition={message.partition()} offset={message.offset()}",
        flush=True,
    )


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    sensor_id = os.getenv("SENSOR_ID", str(uuid.uuid4()))
    interval = env_float("SENSOR_INTERVAL_SECONDS", 2.0)
    temperature_range = (
        env_float("TEMPERATURE_MIN", 20.0),
        env_float("TEMPERATURE_MAX", 50.0),
    )
    humidity_range = (
        env_float("HUMIDITY_MIN", 10.0),
        env_float("HUMIDITY_MAX", 90.0),
    )
    if temperature_range[0] > temperature_range[1]:
        raise ValueError("TEMPERATURE_MIN must not exceed TEMPERATURE_MAX")
    if humidity_range[0] > humidity_range[1]:
        raise ValueError("HUMIDITY_MIN must not exceed HUMIDITY_MAX")

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    print(
        f"Sensor {sensor_id} started; topic={BUILDING_SENSORS_TOPIC}; "
        f"temperature={temperature_range}; humidity={humidity_range}",
        flush=True,
    )

    try:
        while running:
            reading = make_reading(
                sensor_id,
                temperature_range,
                humidity_range,
            )
            payload = json.dumps(reading)
            producer.produce(
                BUILDING_SENSORS_TOPIC,
                key=sensor_id.encode(),
                value=payload.encode(),
                callback=delivery_report,
            )
            producer.poll(0)
            print(f"Generated reading: {payload}", flush=True)
            time.sleep(interval)
    finally:
        print(f"Stopping sensor {sensor_id}; flushing messages...", flush=True)
        undelivered = producer.flush(10)
        if undelivered:
            print(f"Undelivered messages: {undelivered}", flush=True)


if __name__ == "__main__":
    main()
