# GOIT DE HW 06 — Spark Structured Streaming та Kafka

Повністю контейнеризована система моніторингу IoT-показників. Два незалежні
сенсори публікують дані до Kafka, Spark Structured Streaming рахує середню
температуру й вологість у sliding window та надсилає знайдені алерти назад до
Kafka.

## Архітектура

```text
sensor-1 ─┐
          ├─> goit_building_sensors ─> Spark Structured Streaming
sensor-2 ─┘                             │
                                        ├─ watermark: 10 seconds
alerts_conditions.csv ── cross join ────┤  window: 1 minute / slide: 30 seconds
                                        v
                              goit_general_alerts ─> alerts-consumer
```

Kafka працює в KRaft-режимі без ZooKeeper. Один `docker compose up` запускає всі
необхідні компоненти та автоматично створює topics.

## Структура

```text
.
├── data/
│   └── alerts_conditions.csv
├── src/
│   ├── config.py
│   ├── create_topics.py
│   ├── sensor_producer.py
│   ├── spark_streaming.py
│   └── alerts_consumer.py
├── tests/
├── screenshots/
├── compose.yaml
├── Dockerfile
└── requirements.txt
```

## Реалізовані вимоги

1. Повідомлення сенсора містить `id`, `temperature`, `humidity`, `timestamp`.
2. Spark читає JSON із `goit_building_sensors` і використовує event time.
3. Агрегація має watermark **10 секунд**, window **1 хвилину** та slide
   **30 секунд**.
4. Умови читаються зі статичного `data/alerts_conditions.csv` з явною схемою.
5. Після `crossJoin` середні значення фільтруються за умовами CSV. Значення
   `-999` позначає межу, яка не використовується.
6. Результат у JSON записується до `goit_general_alerts` через Spark Kafka sink.

Приклад результату:

```json
{
  "window_start": "2026-10-17T12:00:00.000Z",
  "window_end": "2026-10-17T12:01:00.000Z",
  "temperature_avg": 44.72,
  "humidity_avg": 25.38,
  "code": "104",
  "message": "It's too hot"
}
```

## Запуск

У каталозі `/Users/devops/Downloads/goit-de-hw-06` виконайте:

```bash
docker compose up --build -d
docker compose ps
```

Перший build завантажує Spark Kafka connector і може тривати кілька хвилин.
Kafka доступна контейнерам як `kafka:9092`, а з host — як `localhost:39092`.
Spark UI доступний на <http://localhost:4040>.

Перевірити створення topics:

```bash
docker compose logs topics-init
```

## Перевірка генераторів

Обидва генератори запускаються одночасно разом із Compose:

```bash
docker compose logs -f sensor-1 sensor-2
```

У логах видно два різні `id`, JSON-повідомлення та підтверджені Kafka offsets.
`Ctrl+C` завершує лише перегляд логів.

За замовчуванням демонстраційні сенсори генерують гаряче та сухе середовище,
тому середні значення гарантовано відповідають умовам 101 і 104. Діапазони
можна змінити через `TEMPERATURE_MIN`, `TEMPERATURE_MAX`, `HUMIDITY_MIN` та
`HUMIDITY_MAX` у `compose.yaml`.

## Перевірка Spark та алертів

Переконатися, що Spark query стартував і завантажив CSV:

```bash
docker compose logs spark-processor
```

Переглянути фінальні алерти:

```bash
docker compose logs -f alerts-consumer
```

Через event-time window перший результат не є миттєвим. Spark публікує закрите
вікно після того, як watermark пройде його кінець; за поточних параметрів це
зазвичай займає до **70 секунд**. Кожен рядок consumer містить window,
`temperature_avg`, `humidity_avg`, код, повідомлення, partition і offset.

Прочитати topic напряму Kafka CLI:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic goit_general_alerts \
  --from-beginning \
  --property print.key=true \
  --property key.separator=' | '
```

## Тести

Тести запускаються в образі з Java та PySpark:

```bash
docker compose run --rm --no-deps spark-processor \
  python -m unittest discover -s tests -v
```

Вони перевіряють схему producer-повідомлення, допустимі діапазони, роботу
CSV-умов, одночасне створення двох алертів і включення граничних значень.

## Повторний чистий запуск

Зупинити систему зі збереженням Kafka data та Spark checkpoints:

```bash
docker compose down
```

Очистити topics, offsets і checkpoints для нової демонстрації:

```bash
docker compose down -v
docker compose up --build -d
```

> Після зміни `alerts_conditions.csv` перезапустіть `spark-processor`, оскільки
> статичний CSV читається один раз під час запуску Spark job.
