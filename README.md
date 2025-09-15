# weather-collector
This is a simple application that makes two calls to the OpenWeather weather API for a given location. The first call 
gets the current weather details while the second call gets a 5-day forecast.  Some logic is applied to the data and
then saved to a PostgreSQL database.  This mainly serves as a test application that I can use to work with Kubernetes 
and Docker.  I will use this to practice DevOps using Flux to sync changes to my homelab.  

Full Credits go to Misca van der Burg:  https://www.youtube.com/@mischavandenburg
He has given me the inspiration for creating my homelab.  He also did a very similar Python project for his Shelly 
temperature monitor that can be found here:  https://github.com/mischavandenburg/shelly

Our projects were so similar that I am mostly reusing his README file with some updates for specifics to my project.

Future planned enhancements include:
<ul>
<li>Moving to a single Homelab database that will have persistent data on my Synology NAS</li>
<li>Grafana Dashboard to display the data</li>
<li>Using OpenTelementry for logs, metrics, and traces</li>
<li>Use a Kubernetes CRON instead of a sleep</li>
<li>Consistent logging with Anomaly Detection and Alerting</li>
</ul>

## Project Structure

```bash
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── src/
│   └── weather-collector.py
│   └──configuration/
│      └── configuration.yaml
└── .env
```

## Environment Variables
The following environment variables need to be defined  

```bash
OPENWEATHER_API_KEY
LATITUDE
LONGITUDE
BASE_URL
CREATE_TABLES
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_DB
```

## Error Handling

There is no specific retry logic at this time. If there are errors with one session, this should be logged and it will
retry the same pull for a full 24 hours. 

## Traces, Logs, and Metrics

Logs are exposed as OpenTelemetry.  When running locally, the collector will capture Traces to Tempo, Logs to Splunk, 
and metrics to Prometheus. 

## Docker File

```bash
docker login
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t jaysuzi5/weather-collector:latest \
  --push .
```


### Docker Compose

The `docker-compose.yml` file defines two services:

1. `weather-app`: The main application
2. `postgres-db`: The PostgreSQL database

It also sets up volume persistence for the database and defines a health check for the database service.

## Database Schema

The script creates two tables in the PostgreSQL database `weather_current` with the following schema:

```sql
CREATE TABLE weather_current (
    collection_time TIMESTAMPTZ PRIMARY KEY,
    temperature INTEGER,
    temperature_min INTEGER,
    temperature_max INTEGER,
    humidity INTEGER,
    description VARCHAR(200),
    feels_like INTEGER,
    wind_speed DECIMAL,
    wind_direction INTEGER
);
```

and `weather_forecast` with the following schema:

```sql
CREATE TABLE weather_forecast (
    collection_time TIMESTAMPTZ,
    forecast_date DATE,
    temperature_min INTEGER,
    temperature_max INTEGER,
    humidity_min INTEGER,
    humidity_max INTEGER,
    description VARCHAR(200),
    PRIMARY KEY (collection_time, forecast_date)
);
```
