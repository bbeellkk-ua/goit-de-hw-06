"""Create the Kafka topics required by the Spark Streaming application."""

from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP_SERVERS, TOPICS


def main() -> None:
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing_topics = set(admin_client.list_topics(timeout=10).topics)
    missing_topics = [topic for topic in TOPICS if topic not in existing_topics]

    if missing_topics:
        futures = admin_client.create_topics(
            [
                NewTopic(topic, num_partitions=3, replication_factor=1)
                for topic in missing_topics
            ]
        )
        for topic, future in futures.items():
            future.result(timeout=15)
            print(f"Created topic: {topic}", flush=True)
    else:
        print("All required topics already exist.", flush=True)

    current_topics = admin_client.list_topics(timeout=10).topics
    print("\nHomework topics:", flush=True)
    for topic in TOPICS:
        status = "ready" if topic in current_topics else "missing"
        print(f"- {topic}: {status}", flush=True)


if __name__ == "__main__":
    main()
