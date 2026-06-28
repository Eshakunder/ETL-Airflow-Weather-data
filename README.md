<<<<<<< HEAD
# Airflow-weather-ETL

An automated ETL (Extract, Transform, Load) pipeline built with **Apache Airflow** on **Astronomer** that fetches real-time weather data from the [Open-Meteo API](https://open-meteo.com/) and stores it in a **PostgreSQL** database.
 
---
 
##  Tech Stack
 
- **Apache Airflow** (via Astronomer CLI) — workflow orchestration
- **Open-Meteo API** — free, open-source weather API (no key required)
- **PostgreSQL** — data storage
- **Python** — ETL logic
---
 
##  Project Structure
 
```
etl-weather-pipeline/
├── dags/
│   └── etlweather.py       # Main ETL DAG
├── requirements.txt        # Python dependencies
├── Dockerfile              # Astronomer Docker config
└── README.md
```
 
---
 
##  Pipeline Overview
 
The DAG `weather_etl_pipeline` runs daily and consists of 3 tasks:
 
1. **Extract** — Fetches current weather data from Open-Meteo API for London (lat: 51.5074, lon: -0.1278)
2. **Transform** — Parses and structures the API response
3. **Load** — Inserts the transformed data into a PostgreSQL table
---
 
##  Getting Started
 
### Prerequisites
- [Docker](https://www.docker.com/)
- [Astronomer CLI](https://docs.astronomer.io/astro/cli/install-cli)
### Installation
 
```bash
# Clone the repo
git clone https://github.com/your-username/etl-weather-pipeline.git
cd etl-weather-pipeline
 
# Start Airflow
astro dev start
```
 
### Set up Airflow Connections
 
Go to **Admin → Connections** in the Airflow UI and add:
 
**1. Open-Meteo API**
| Field | Value |
|---|---|
| Connection ID | `open_meteo_api` |
| Connection Type | `HTTP` |
| Host | `https://api.open-meteo.com` |
 
**2. PostgreSQL**
| Field | Value |
|---|---|
| Connection ID | `postgres_default` |
| Connection Type | `Postgres` |
| Host | `host.docker.internal` |
| Port | *(check with `astro dev ps`)* |
| Database | `postgres` |
| Login | `postgres` |
| Password | `postgres` |
 
### Trigger the DAG
 
1. Open Airflow UI at `http://etlweather.localhost:<port>`
2. Enable and trigger `weather_etl_pipeline`
---
 
##  Database Schema
 
```sql
CREATE TABLE weather_data (
    latitude        FLOAT,
    longitude       FLOAT,
    temperature     FLOAT,
    windspeed       FLOAT,
    winddirection   FLOAT,
    weathercode     INT,
    time            TIMESTAMP
);
```
 
---
 
##  Exporting Data
 
To export all data to CSV:
 
```bash
docker exec -it $(docker ps | grep postgres | awk '{print $1}') \
  psql -U postgres -c "\COPY weather_data TO '/tmp/weather_data.csv' CSV HEADER;"
 
docker cp $(docker ps | grep postgres | awk '{print $1}'):/tmp/weather_data.csv \
  ~/Desktop/weather_data_$(date +%Y%m%d_%H%M%S).csv
```
 
---
 
##  Stopping the Project
 
```bash
astro dev stop
```
 
---
 
