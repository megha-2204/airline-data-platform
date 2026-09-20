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
KAFKA_TOPIC = "airline_bookings"


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def main():
    producer = create_kafka_producer()

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            booking_id,
            customer_id,
            flight_id,
            booking_time,
            seat_count,
            total_amount,
            status
        FROM airline.bookings
        ORDER BY booking_id
        """
    )

    rows = cursor.fetchall()

    print(f"Found {len(rows)} bookings.")

    for row in rows:
        event = {
            "booking_id": row[0],
            "customer_id": row[1],
            "flight_id": row[2],
            "booking_time": row[3].isoformat() if row[3] else None,
            "seat_count": row[4],
            "total_amount": float(row[5]),
            "status": row[6],
        }

        producer.send(KAFKA_TOPIC, value=event)

        print(f"Published booking event: {event}")

        time.sleep(0.1)

    producer.flush()

    cursor.close()
    conn.close()
    producer.close()

    print("All booking events published successfully.")


if __name__ == "__main__":
    main()