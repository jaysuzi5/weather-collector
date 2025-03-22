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
└── .env
```

## Local Installation and Setup

1. Clone this repository

2. Create a `.env` file in the project root with the following variables:

   ```bash
    LATITUDE=<your-weather-location-latitude>
    LONGITUDE=<your-weather-location-longitude>
    WEATHER_SLEEP_SECONDS=<time-to-sleep-between-weather-calls-in-seconds>
    OPENWEATHER_API_KEY=<your-open-weather-api-key>
    POSTGRES_USER=<your-postgresql-username>
    POSTGRES_PASSWORD=<your-postgresql-password>
    POSTGRES_DB=<<your-postgresql-database-name>>
   ```

## Usage

To run the application using Docker Compose:

```bash
docker-compose up --build
```

This command will:

1. Build the Docker image for the Weather Collector application
2. Start a PostgreSQL container
3. Start the Weather Collector container
4. Connect the OpenWeather API and collect the data
5. Log the data to the PostgreSQL database

To stop the application:

```bash
docker-compose down
```

To stop the application and remove the volumes (this will delete the database data):

```bash
docker-compose down -v
```

When running on Docker on MacOS, cycles can be missed when the system hibernates.  To prevent this, you can use 
caffeinate to keep it active

```bash
caffeinate -d docker attach weather-app-container
```

## Docker Configuration

### Dockerfile

The Dockerfile sets up the Python environment and installs the necessary dependencies:

```dockerfile
FROM python:3.12.5-alpine3.20
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ .
CMD ["python", "main.py"]
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


## Error Handling

There is no specific retry logic at this time.  If either the API or PostgreSQL fails, an ERROR log statement will
be created.  It will try again on the next cycle. 

## Logging

The script uses Python's built-in logging module to provide informative logs about its operation. Logs are printed to 
the console with timestamps and can be viewed using `docker-compose logs mqtt-client`.