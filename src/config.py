"""Shared configuration for the Kafka and Spark homework components."""

import os


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "goit")

BUILDING_SENSORS_TOPIC = f"{TOPIC_PREFIX}_building_sensors"
GENERAL_ALERTS_TOPIC = f"{TOPIC_PREFIX}_general_alerts"
TOPICS = (BUILDING_SENSORS_TOPIC, GENERAL_ALERTS_TOPIC)

UNUSED_LIMIT = -999.0
