CREATE SCHEMA IF NOT EXISTS airline;

CREATE TABLE airline.airports (
                                  airport_id SERIAL PRIMARY KEY,
                                  airport_code VARCHAR(10) UNIQUE NOT NULL,
                                  city VARCHAR(100) NOT NULL,
                                  country VARCHAR(100) NOT NULL
);

CREATE TABLE airline.aircraft (
                                  aircraft_id SERIAL PRIMARY KEY,
                                  aircraft_type VARCHAR(100) NOT NULL,
                                  total_seats INT NOT NULL
);

CREATE TABLE airline.customers (
                                   customer_id SERIAL PRIMARY KEY,
                                   customer_name VARCHAR(100) NOT NULL,
                                   email VARCHAR(200) UNIQUE NOT NULL,
                                   country VARCHAR(100),
                                   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE airline.flights (
                                 flight_id SERIAL PRIMARY KEY,
                                 flight_number VARCHAR(20) UNIQUE NOT NULL,
                                 origin_airport_id INT NOT NULL,
                                 destination_airport_id INT NOT NULL,
                                 aircraft_id INT NOT NULL,
                                 departure_time TIMESTAMP NOT NULL,
                                 arrival_time TIMESTAMP NOT NULL,

                                 FOREIGN KEY (origin_airport_id)
                                     REFERENCES airline.airports(airport_id),

                                 FOREIGN KEY (destination_airport_id)
                                     REFERENCES airline.airports(airport_id),

                                 FOREIGN KEY (aircraft_id)
                                     REFERENCES airline.aircraft(aircraft_id)
);

CREATE TABLE airline.searches (
                                  search_id BIGSERIAL PRIMARY KEY,
                                  customer_id INT,
                                  flight_id INT NOT NULL,
                                  searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                                  FOREIGN KEY (customer_id)
                                      REFERENCES airline.customers(customer_id),

                                  FOREIGN KEY (flight_id)
                                      REFERENCES airline.flights(flight_id)
);

CREATE TABLE airline.bookings (
                                  booking_id BIGSERIAL PRIMARY KEY,
                                  customer_id INT NOT NULL,
                                  flight_id INT NOT NULL,
                                  booking_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                  seat_count INT NOT NULL,
                                  total_amount NUMERIC(12,2) NOT NULL,
                                  status VARCHAR(30) NOT NULL,

                                  FOREIGN KEY (customer_id)
                                      REFERENCES airline.customers(customer_id),

                                  FOREIGN KEY (flight_id)
                                      REFERENCES airline.flights(flight_id)
);

CREATE TABLE airline.payments (
                                  payment_id BIGSERIAL PRIMARY KEY,
                                  booking_id BIGINT NOT NULL,
                                  payment_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                  amount NUMERIC(12,2) NOT NULL,
                                  payment_status VARCHAR(30) NOT NULL,

                                  FOREIGN KEY (booking_id)
                                      REFERENCES airline.bookings(booking_id)
);

CREATE TABLE airline.cancellations (
                                       cancellation_id BIGSERIAL PRIMARY KEY,
                                       booking_id BIGINT NOT NULL,
                                       cancelled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                       refund_amount NUMERIC(12,2),

                                       FOREIGN KEY (booking_id)
                                           REFERENCES airline.bookings(booking_id)
);