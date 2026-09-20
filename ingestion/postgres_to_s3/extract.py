import psycopg2
import csv
import os


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "airline_db",
    "user": "airline_user",
    "password": "airline_pass"
}


TABLES = [
    "airports",
    "aircraft",
    "customers",
    "flights",
    "searches",
    "bookings",
    "payments",
    "cancellations"
]


OUTPUT_DIR = "data"


def extract_table(conn, table_name):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cursor = conn.cursor()

    query = f"SELECT * FROM airline.{table_name}"

    cursor.execute(query)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    output_file = os.path.join(
        OUTPUT_DIR,
        f"{table_name}.csv"
    )

    with open(output_file, "w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow(columns)
        writer.writerows(rows)

    cursor.close()

    print(
        f"Extracted {len(rows)} rows from "
        f"airline.{table_name} → {output_file}"
    )


def main():

    conn = psycopg2.connect(**DB_CONFIG)

    for table in TABLES:
        extract_table(conn, table)

    conn.close()

    print("PostgreSQL extraction completed.")


if __name__ == "__main__":
    main()