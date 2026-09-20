import json
import time

import psycopg2
from kafka import KafkaProducer


POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "airline_db",
    "user": "airline_user",
    "password": "airline_pass",
}

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "airline_searches"


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def main():
    producer = create_kafka_producer()

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            search_id,
            customer_id,
            flight_id,
            searched_at
        FROM airline.searches
        ORDER BY search_id
        """
    )

    rows = cursor.fetchall()

    print(f"Found {len(rows)} searches.")

    for row in rows:
        search_id = row[0]

        event = {
            "search_id": search_id,
            "customer_id": row[1],
            "flight_id": row[2],
            "searched_at": row[3].isoformat() if row[3] else None,
        }

        producer.send(
            KAFKA_TOPIC,
            key=search_id,
            value=event,
        )

        print(f"Published search event: {event}")

        time.sleep(0.1)

    producer.flush()

    cursor.close()
    conn.close()
    producer.close()

    print("All search events published successfully.")


if __name__ == "__main__":
    main()