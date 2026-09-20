import random
import time
import psycopg2
from faker import Faker
import argparse

fake = Faker()

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "airline_db",
    "user": "airline_user",
    "password": "airline_pass",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def seed_data():
    conn = get_connection()
    cur = conn.cursor()

    # -------------------------
    # Airports
    # -------------------------
    airports = [
        ("MAA", "Chennai", "India"),
        ("BLR", "Bengaluru", "India"),
        ("DEL", "Delhi", "India"),
        ("BOM", "Mumbai", "India"),
        ("SIN", "Singapore", "Singapore"),
        ("DXB", "Dubai", "UAE"),
        ("LHR", "London", "UK"),
        ("BKK", "Bangkok", "Thailand"),
    ]

    for code, city, country in airports:
        cur.execute(
            """
            INSERT INTO airline.airports
                (airport_code, city, country)
            VALUES (%s, %s, %s)
                ON CONFLICT (airport_code) DO NOTHING
            """,
            (code, city, country),
        )

    # -------------------------
    # Aircraft
    # -------------------------
    aircraft_types = [
        ("Airbus A320", 180),
        ("Boeing 737", 189),
        ("Airbus A321", 220),
        ("Boeing 787", 250),
    ]

    for aircraft_type, seats in aircraft_types:
        cur.execute(
            """
            INSERT INTO airline.aircraft
                (aircraft_type, total_seats)
            VALUES (%s, %s)
            """,
            (aircraft_type, seats),
        )

    # -------------------------
    # Customers
    # -------------------------
    for _ in range(500):
        cur.execute(
            """
            INSERT INTO airline.customers
                (customer_name, email, country)
            VALUES (%s, %s, %s)
                ON CONFLICT (email) DO NOTHING
            """,
            (
                fake.name(),
                fake.unique.email(),
                random.choice(
                    ["India", "Singapore", "UAE", "UK", "Thailand"]
                ),
            ),
        )

    conn.commit()

    print("Reference data seeded.")

    # -------------------------
    # Get airport IDs
    # -------------------------
    cur.execute(
        "SELECT airport_id, airport_code FROM airline.airports"
    )

    airport_rows = cur.fetchall()

    airport_map = {
        code: airport_id
        for airport_id, code in airport_rows
    }

    # -------------------------
    # Get aircraft IDs
    # -------------------------
    cur.execute(
        "SELECT aircraft_id FROM airline.aircraft"
    )

    aircraft_ids = [row[0] for row in cur.fetchall()]

    # -------------------------
    # Routes
    # -------------------------
    routes = [
        ("MAA", "SIN"),
        ("BLR", "SIN"),
        ("DEL", "DXB"),
        ("BOM", "LHR"),
        ("MAA", "DXB"),
        ("BLR", "BKK"),
    ]

    # -------------------------
    # Flights
    # -------------------------
    for i in range(30):

        origin, destination = random.choice(routes)

        departure_hours = random.randint(6, 168)
        flight_duration = random.randint(2, 8)
        arrival_hours = departure_hours + flight_duration

        cur.execute(
            """
            INSERT INTO airline.flights
            (
                flight_number,
                origin_airport_id,
                destination_airport_id,
                aircraft_id,
                departure_time,
                arrival_time
            )
            VALUES (
                       %s,
                       %s,
                       %s,
                       %s,
                       CURRENT_TIMESTAMP + (%s || ' hours')::interval,
                       CURRENT_TIMESTAMP + (%s || ' hours')::interval
                   )
                ON CONFLICT (flight_number) DO NOTHING
            """,
            (
                f"AI{i+100}",
                airport_map[origin],
                airport_map[destination],
                random.choice(aircraft_ids),
                departure_hours,
                arrival_hours,
            ),
        )

    conn.commit()

    print("Flights seeded.")

    # -------------------------
    # Close database resources
    # -------------------------
    cur.close()
    conn.close()


def generate_search(cur):
    cur.execute(
        """
        SELECT customer_id
        FROM airline.customers
        ORDER BY RANDOM()
            LIMIT 1
        """
    )
    customer_id = cur.fetchone()[0]

    cur.execute(
        """
        SELECT flight_id
        FROM airline.flights
        WHERE departure_time > CURRENT_TIMESTAMP
        ORDER BY RANDOM()
            LIMIT 1
        """
    )
    flight = cur.fetchone()

    if not flight:
        return

    flight_id = flight[0]

    cur.execute(
        """
        INSERT INTO airline.searches
            (customer_id, flight_id)
        VALUES (%s, %s)
        """,
        (customer_id, flight_id),
    )

def generate_booking(cur):
    # Select a random customer
    cur.execute(
        """
        SELECT customer_id
        FROM airline.customers
        ORDER BY RANDOM()
            LIMIT 1
        """
    )
    customer_id = cur.fetchone()[0]

    # Select a future flight
    cur.execute(
        """
        SELECT flight_id
        FROM airline.flights
        WHERE departure_time > CURRENT_TIMESTAMP
        ORDER BY RANDOM()
            LIMIT 1
        """
    )
    flight = cur.fetchone()

    if not flight:
        return None

    flight_id = flight[0]

    # Generate booking details
    seat_count = random.randint(1, 4)

    price_per_seat = random.randint(4000, 15000)

    total_amount = seat_count * price_per_seat

    status = "CONFIRMED"

    cur.execute(
        """
        INSERT INTO airline.bookings
        (
            customer_id,
            flight_id,
            seat_count,
            total_amount,
            status
        )
        VALUES (%s, %s, %s, %s, %s)
            RETURNING booking_id
        """,
        (
            customer_id,
            flight_id,
            seat_count,
            total_amount,
            status,
        ),
    )

    booking_id = cur.fetchone()[0]

    return booking_id, total_amount

def generate_payment(cur, booking_id, amount):
    cur.execute(
        """
        INSERT INTO airline.payments
        (
            booking_id,
            amount,
            payment_status
        )
        VALUES (%s, %s, %s)
        """,
        (
            booking_id,
            amount,
            "SUCCESS",
        ),
    )

def generate_cancellation(cur, booking_id, amount):
    refund_amount = amount

    cur.execute(
        """
        INSERT INTO airline.cancellations
        (
            booking_id,
            refund_amount
        )
        VALUES (%s, %s)
        """,
        (
            booking_id,
            refund_amount,
        ),
    )

    cur.execute(
        """
        UPDATE airline.bookings
        SET status = 'CANCELLED'
        WHERE booking_id = %s
        """,
        (booking_id,),
    )

def generate_transactions(count):
    conn = get_connection()
    cur = conn.cursor()

    booking_ids = []

    for i in range(count):

        # Every transaction generates a search
        generate_search(cur)

        # Roughly 30% of searches become bookings
        if random.random() < 0.30:

            booking = generate_booking(cur)

            if booking:
                booking_id, amount = booking

                # Successful payment
                generate_payment(
                    cur,
                    booking_id,
                    amount
                )

                booking_ids.append(
                    (booking_id, amount)
                )

        # Commit periodically
        if (i + 1) % 100 == 0:
            conn.commit()
            print(f"Generated {i + 1} events")

    conn.commit()

    # Cancel roughly 10% of bookings
    cancellation_count = max(
        1,
        int(len(booking_ids) * 0.10)
    )

    selected_bookings = random.sample(
        booking_ids,
        min(cancellation_count, len(booking_ids))
    )

    for booking_id, amount in selected_bookings:
        generate_cancellation(
            cur,
            booking_id,
            amount
        )

    conn.commit()

    cur.close()
    conn.close()

    print("Transactional data generation completed.")

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--seed",
        action="store_true",
        help="Seed reference data and flights"
    )

    parser.add_argument(
        "--generate",
        type=int,
        help="Generate a fixed number of transactional events"
    )

    parser.add_argument(
        "--stream",
        action="store_true",
        help="Continuously generate flight searches"
    )

    args = parser.parse_args()

    if args.seed:
        seed_data()
        print("Seeding completed.")
        return

    if args.generate:
        generate_transactions(args.generate)
        return

    if args.stream:
        print("Starting flight search stream...")

        while True:
            conn = get_connection()
            cur = conn.cursor()

            generate_search(cur)

            conn.commit()

            cur.close()
            conn.close()

            print("Generated flight search")
            time.sleep(1)

        return

    print("Use --seed, --generate N, or --stream")

if __name__ == "__main__":
    main()