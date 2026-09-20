# ✈️ Airline Data Engineering Platform

An end-to-end airline data engineering platform that processes flight search, booking, payment, cancellation, and real-time demand data using batch and streaming data pipelines.

The platform combines PostgreSQL, Amazon S3, Snowflake, dbt, Apache Airflow, Apache Kafka, Apache Spark Structured Streaming, and Tableau to demonstrate a modern data engineering workflow.

---

## 📌 Project Overview

This project simulates an airline booking data platform.

PostgreSQL is used as the operational OLTP source. Data is processed through both batch and real-time pipelines.

The batch pipeline moves data from PostgreSQL to Amazon S3 and then into Snowflake, where dbt is used to transform the data into analytical models.

The streaming pipeline uses Kafka and Spark Structured Streaming to process flight search and booking events in near-real-time.

The processed demand signals are used to simulate dynamic flight pricing based on occupancy, search activity, booking activity, and time to departure.

---

# 🏗️ Architecture

## Batch Pipeline

```text
PostgreSQL
    │
    ▼
Python Extraction
    │
    ▼
Amazon S3
    │
    ▼
Snowflake RAW
    │
    ▼
dbt
    │
    ▼
Analytical Marts
    │
    ▼
Tableau
```

### Real-Time Pipeline

```text
PostgreSQL / Event Producers
        │
        ▼
      Kafka
        │
        ▼
Spark Structured Streaming
        │
        ▼
Demand & Pricing Engine
        │
        ▼
    Snowflake
        │
        ▼
       dbt
        │
        ▼
     Tableau
     
```

## Complete Platform

```text
                         ┌──────────────────────┐
                         │     PostgreSQL       │
                         │      OLTP Source     │
                         └──────────┬───────────┘
                                    │
                 ┌──────────────────┴──────────────────┐
                 │                                     │
              Batch                                Streaming
                 │                                     │
                 ▼                                     ▼
        ┌─────────────────┐                    ┌───────────────┐
        │       S3        │                    │     Kafka     │
        │   Raw Storage   │                    │ Event Stream  │
        └────────┬────────┘                    └───────┬───────┘
                 │                                     │
                 ▼                                     ▼
        ┌─────────────────┐                    ┌───────────────┐
        │    Snowflake    │                    │     Spark     │
        │   RAW Layer     │                    │   Streaming   │
        └────────┬────────┘                    └───────┬───────┘
                 │                                     │
                 ▼                                     ▼
        ┌─────────────────┐                    ┌───────────────┐
        │      dbt        │                    │ Demand &      │
        │ Transformations │                    │ Pricing Engine│
        └────────┬────────┘                    └───────┬───────┘
                 │                                     │
                 │                                     ▼
                 │                            ┌───────────────┐
                 │                            │   Snowflake   │
                 │                            │ FLIGHT_PRICING│
                 │                            └───────┬───────┘
                 │                                    │
                 └────────────────┬───────────────────┘
                                  ▼
                         ┌─────────────────┐
                         │ flight_pricing  │
                         │      _mart      │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │     Tableau     │
                         │   Dashboard     │
                         └─────────────────┘

                            Apache Airflow
                    orchestrates the batch pipeline
```

## 🎯 Business Problem

Airlines need to continuously monitor passenger demand to understand how interest in a flight changes as the departure date approaches.

This project simulates a data platform that captures:

```text
Flight searches
Flight bookings
Seat occupancy
Payment transactions
Cancellations
Real-time booking events
Real-time search events
```

These signals are used to calculate flight demand and simulate dynamic pricing.

The pricing model considers:

```text
Occupancy
Search activity
Booking activity
Search velocity
Booking velocity
Time to departure
```

The goal is to demonstrate how a modern data engineering platform can combine batch and streaming data to support analytical and near-real-time decision making.

The pricing engine is a simulation created for this project and does not represent a production airline pricing algorithm.
## Technology Stack

| Technology | Purpose |
|---|---|
| PostgreSQL | OLTP source for airline booking data |
| Python | Data generation and ingestion |
| Faker | Synthetic airline data generation |
| Amazon S3 | Raw data lake / object storage |
| Snowflake | Analytical data warehouse |
| dbt | SQL transformations and data modeling |
| Apache Airflow | Pipeline orchestration |
| Apache Kafka | Real-time event streaming |
| Apache Spark | Real-time stream processing |
| Tableau | Analytics and dashboards |
| Docker | Local infrastructure and services |
| Git / GitHub | Version control |

### 🔄 Data Flow

The platform contains two complementary data pipelines.

### 1. Batch Data Pipeline

Operational data is extracted from PostgreSQL and loaded into the analytical platform.

```text

PostgreSQL
    │
    ▼
Python Extraction
    │
    ▼
CSV Files
    │
    ▼
Amazon S3
    │
    ▼
Snowflake RAW
    │
    ▼
dbt Staging
    │
    ▼
dbt Intermediate
    │
    ▼
dbt Marts
    │
    ▼
Tableau
```

### 2. Real-Time Data Pipeline

Flight search and booking events are streamed through Kafka and processed using Spark Structured Streaming.

```text

Search / Booking Events
          │
          ▼
        Kafka
          │
          ▼
Spark Structured Streaming
          │
          ▼
    Demand Calculation
          │
          ▼
    Dynamic Pricing
          │
          ▼
        Snowflake
          │
          ▼
    dbt Analytical Mart
          │
          ▼
        Tableau
```

### 3. Pipeline Orchestration

Apache Airflow orchestrates the batch workflow:

```text
PostgreSQL
    │
    ▼
PostgreSQL → S3
    │
    ▼
S3 → Snowflake
    │
    ▼
dbt Transformations

```
### 🗄️ Source Data Model

PostgreSQL acts as the operational OLTP source.

The database contains the following tables:

```text

airline
│
├── airports
├── aircraft
├── customers
├── flights
├── searches
├── bookings
├── payments
└── cancellations
```

Example relationships:

```text
Customers ──────► Bookings ──────► Flights
                      │
                      ▼
                   Payments

Searches ─────────────► Flights

Bookings ─────────────► Cancellations
```

### ❄️ Snowflake Data Warehouse

Snowflake is used as the analytical data warehouse for the platform.

The data is organized into layers based on its purpose.

```text
PostgreSQL / S3
       │
       ▼
   Snowflake
       │
       ▼
      RAW
       │
       ▼
      dbt
       │
       ├── STAGING
       │
       ├── INTERMEDIATE
       │
       └── MARTS
```

### RAW Layer

The RAW schema contains data loaded from the source systems with minimal transformation.

The project loads the following datasets:

```text
Airports
Aircraft
Customers
Flights
Searches
Bookings
Payments
Cancellations
Flight pricing
Real-Time Pricing Data
```

Spark writes calculated pricing results into the FLIGHT_PRICING table.

The table contains:

```text
Flight ID
Total seats
Departure time
Days to departure
Booking count
Seats booked
Occupancy
Search count
Search signal
Booking revenue
Demand score
Price multiplier
Simulated price
Processing timestamp
```


The pricing data is then exposed through the dbt staging model stg_flight_pricing and incorporated into flight_pricing_mart.

### 🔄 dbt Transformation Layer

dbt is used to transform the raw Snowflake data into structured analytical models.

The project follows a layered modeling approach:

```text

Snowflake RAW
│
▼
STAGING
│
▼
INTERMEDIATE
│
▼
MARTS
```

### Staging Models

The staging layer provides a clean representation of the raw source tables.

Examples include:

```text
stg_airports
stg_aircraft
stg_customers
stg_flights
stg_searches
stg_bookings
stg_payments
stg_cancellations
stg_flight_pricing
```

### Intermediate Models

The intermediate layer contains business transformations used to calculate flight performance and demand.

Examples include:

```text
int_flight_performance
int_flight_demand
int_dynamic_pricing
```

### Analytical Marts

The mart layer contains business-ready datasets designed for analytics and reporting.

The main analytical mart is:

flight_pricing_mart

It combines:

```text
Flight performance
Seat occupancy
Booking information
Search activity
Demand score
Price multiplier
Simulated price
Revenue metrics
```

The mart provides a single analytical dataset for the Tableau dashboard.

```text
  Staging
    │
    ▼
 Intermediate
    │
    ▼
flight_pricing_mart
    │
    ▼
  Tableau
```

### 🔄 Airflow Orchestration

Apache Airflow is used to orchestrate the batch data pipeline.

The main workflow is:

```text

postgres_to_s3
│
▼
s3_to_snowflake
│
▼
dbt_run

```

The DAG manages dependencies between the ingestion and transformation stages, ensuring that downstream tasks execute after their upstream dependencies complete successfully.

Airflow is responsible for orchestration and scheduling, while dbt is responsible for data transformation and modeling.

## ⚡ Kafka & Spark Streaming

Kafka is used as the event streaming layer for real-time airline activity.

The project uses separate Kafka topics for flight search and booking events:

```text
airline_searches
airline_bookings
```

Event producers publish simulated airline search and booking activity to these Kafka topics.

Spark Structured Streaming consumes these events and processes them to calculate real-time demand signals.

```text
Event Producers
│
▼
Kafka
│
▼
Spark Structured Streaming
│
▼
Demand Signals
│
▼
Dynamic Pricing
│
▼
Snowflake
```

The streaming pipeline allows search and booking activity to influence the simulated pricing calculation.

**💰 Dynamic Pricing Logic**

The project implements a simulated demand-based pricing model.

The demand score combines three signals:

Demand Score =
Search Signal    × 40%
+ Booking Signal   × 40%
+ Occupancy Signal × 20%

The signals are normalized before being combined into the demand score.

The resulting demand score is converted into a price multiplier:

Price Multiplier =
1 + (Demand Score × 0.5)

The multiplier is constrained between:

1.00x ─────────────── 1.50x

The simulated price is calculated using:

```text
Simulated Price =
Average Booking Value × Price Multiplier
```

Example:

Average Booking Value = ₹20,000
Demand Score          = 0.70

Price Multiplier      = 1.35

Simulated Price       = ₹27,000

The pricing model demonstrates how real-time demand signals can be transformed into a business-oriented pricing metric.

**📊 Tableau Dashboard**

The processed data is visualized using Tableau to provide an analytical view of flight demand, occupancy, bookings, revenue, and simulated dynamic pricing.

**Dashboard: Airline Demand & Dynamic Pricing Overview**

The dashboard includes:

```text
Total number of flights
Total bookings
Average occupancy
Flight demand
Search activity
Booking activity
Simulated flight pricing
Revenue analysis
Base revenue vs simulated adjusted revenue
```

The dashboard helps analyze the relationship between passenger demand, occupancy, and simulated pricing.

```text
Demand Signals
│
├── Searches
├── Bookings
└── Occupancy
│
▼
Demand Score
│
▼
Price Multiplier
│
▼
Simulated Price
│
▼
Revenue Analysis
```


## 🧪 Data Validation & Testing

The pipeline was validated at each major stage to ensure that data was successfully transferred and transformed.

**Source Validation**

PostgreSQL was validated using row counts across the source tables.

Example datasets include:

```text
8 airports
4 aircraft
500 customers
30 flights
1,000 searches
309 bookings
309 payments
30 cancellations
```


### Snowflake Validation

The corresponding Snowflake RAW tables were validated against the source datasets after ingestion.

`dbt Validation
`

dbt models were validated using:

```text
dbt debug
dbt parse
dbt run

```

The staging, intermediate, and mart models were successfully built.

## Streaming Validation

Kafka and Spark Structured Streaming were validated by processing search and booking events and writing pricing results to Snowflake.

The FLIGHT_PRICING table was validated for:

```text
Demand score
Occupancy
Booking count
Search count
Price multiplier
Simulated price
Analytical Validation
```

The final flight_pricing_mart was validated before being connected to Tableau to ensure that the dashboard was consuming the expected analytical results.

```text


📁 Project Structure
airline-data-platform/
│
├── docker-compose.yml
│
├── postgres/
│   └── schema.sql
│
├── generator/
│   ├── generator.py
│   └── requirements.txt
│
├── ingestion/
│   └── postgres_to_s3/
│       └── extract.py
│
├── dbt/
│   └── airline_dbt/
│       ├── dbt_project.yml
│       └── models/
│           ├── staging/
│           │   ├── staging.yml
│           │   ├── stg_airports.sql
│           │   ├── stg_aircraft.sql
│           │   ├── stg_customers.sql
│           │   ├── stg_flights.sql
│           │   ├── stg_searches.sql
│           │   ├── stg_bookings.sql
│           │   ├── stg_payments.sql
│           │   ├── stg_cancellations.sql
│           │   └── stg_flight_pricing.sql
│           │
│           ├── intermediate/
│           │   ├── int_flight_performance.sql
│           │   ├── int_flight_demand.sql
│           │   └── int_dynamic_pricing.sql
│           │
│           └── marts/
│               └── flight_pricing_mart.sql
│
└── streaming/
├── kafka/
│   ├── docker-compose.yml
│   ├── booking_producer.py
│   └── search_producer.py
│
└── spark/
├── demand_stream.py
└── search_demand.py

```

### 🚀 Getting Started

**Prerequisites**

Make sure the following tools are installed:

```text
Docker Desktop
Python 3.12+
Git
AWS CLI
Snowflake account
dbt
Apache Kafka
Apache Spark
Tableau Desktop
```

1. Clone the Repository

`git clone https://github.com/<YOUR_USERNAME>/airline-data-platform.git`

`cd airline-data-platform`


2. Start PostgreSQL

Start the PostgreSQL container:

`docker compose up -d
`

Verify that the container is running:

`docker ps
`

3.Generate Source Data

Navigate to the generator:

`cd generator
`

Create and activate the Python environment:

`python3 -m venv .venv`

`source .venv/bin/activate`

**Install dependencies:**

`pip install -r requirements.txt
`

**Generate airline data:**

`python generator.py --generate 1000
`

**4. Extract Data from PostgreSQL**

Navigate to the ingestion pipeline:

`cd ../ingestion/postgres_to_s3
`

Run the extraction script:

python extract.py

This extracts the PostgreSQL datasets into local files for the batch ingestion process.

**5. Load Data into Amazon S3**

Upload the extracted datasets to the configured S3 bucket:

aws s3 cp data/ s3://<YOUR_BUCKET>/raw/ --recursive

Replace <YOUR_BUCKET> with your own S3 bucket name.

**6. Load Data into Snowflake**

The extracted datasets are loaded into the Snowflake RAW schema.

After loading, validate the tables using SQL queries such as:

SELECT COUNT(*)
FROM AIRLINE_DB.RAW.FLIGHTS;

**7. Run dbt**

Navigate to the dbt project:

cd ../../dbt/airline_dbt

**Validate the dbt configuration:**

`dbt debug
`

**Parse the project:**

`dbt parse
`

**Run the transformations:**

`dbt run`

8. Start Kafka

Navigate to the Kafka directory:

`cd ../../streaming/kafka`

Start Kafka:

`docker compose up -d`

The streaming pipeline uses:

```text
airline_searches
airline_bookings
```

as Kafka topics.

**9. Run Spark Streaming**

Configure Java 17:

```text
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1
```

Run the Spark streaming application from the streaming/spark directory.

The Spark pipeline consumes Kafka events, calculates demand signals, applies the simulated pricing logic, and writes pricing results to Snowflake.

**10. Run Airflow**

The Airflow DAG orchestrates the batch workflow:

```text
PostgreSQL
│
▼
S3
│
▼
Snowflake
│
▼
dbt
```

The DAG can be triggered from the Airflow UI after the required connections and environment variables have been configured.

**11. View the Tableau Dashboard**

Connect Tableau to the final Snowflake analytical dataset and open the published dashboard:

## Airline Demand & Dynamic Pricing Overview

**🔐 Security & Configuration**

Credentials and environment-specific configuration are kept outside the Git repository.

The following types of files are excluded through .gitignore:

```text
.env
*.pem
*.key
.venv/
data/
*.csv
*.log
.idea/
checkpoint/
```

_Cloud credentials, Snowflake passwords, and other secrets should be supplied through environment variables or local configuration rather than committed to source control._

**Example:**

export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export SNOWFLAKE_USER="..."
export SNOWFLAKE_PASSWORD="..."

Never commit actual credentials or secrets to GitHub.

🎓 Key Engineering Concepts Demonstrated

This project demonstrates practical experience with:

```text
OLTP vs analytical workloads
Data lake architecture
Cloud object storage
Data warehouse architecture
Batch ETL/ELT pipelines
SQL data modeling
dbt layered transformations
Workflow orchestration
Event-driven architecture
Kafka producers and topics
Spark Structured Streaming
Real-time aggregation
Demand signal calculation
Dynamic pricing simulation
Analytical marts
BI dashboards
Dockerized infrastructure
Git-based version control
```

## 📌 Project Outcomes

The completed platform demonstrates an end-to-end data engineering workflow:

```text
Generate
   ↓
Store
   ↓
Ingest
   ↓
Transform
   ↓
Orchestrate
   ↓
Stream
   ↓
Process
   ↓
Model
   ↓
Analyze
   ↓
Visualize
```

The project combines batch and streaming architectures to demonstrate how modern data platforms can support both analytical workloads and near-real-time business use cases.

## 👩‍💻 Author


**Megha**

Data Engineering | SQL | Python | Snowflake | dbt | Airflow | Kafka | Spark | AWS

- GitHub: [Your GitHub Profile](https://github.com/megha-2204)
- LinkedIn: [Your LinkedIn Profile](www.linkedin.com/in/megha-b-951458218)