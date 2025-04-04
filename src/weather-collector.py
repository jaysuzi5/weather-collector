from datetime import date, datetime
from time import sleep
import logging
import decimal
import psycopg2
import os
import requests
import json
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

class WeatherAPIWrapper:
    """
    A wrapper class for interacting with a weather API, specifically OpenWeatherMap.

    This class provides methods to retrieve current weather conditions and
    forecast information based on latitude and longitude coordinates.
    """

    def __init__(self, api_key, base_url="http://api.openweathermap.org/data/2.5/"):
        """
        Initializes the WeatherAPIWrapper.

        Args:
            api_key (str): Your API key for the OpenWeatherMap service.
            base_url (str, optional): The base URL of the OpenWeatherMap API.
                                       Defaults to OpenWeatherMap's current weather URL.
        """
        self.api_key = api_key
        self.base_url = base_url

    def _build_url(self, endpoint, params):
        """
        Constructs the full API URL for a given endpoint and parameters.

        Args:
            endpoint (str): The API endpoint (e.g., 'weather', 'forecast').
            params (dict): A dictionary of query parameters.

        Returns:
            str: The complete API URL.
        """
        url = f"{self.base_url}{endpoint}"
        params["appid"] = self.api_key  # Add API key to all requests
        query_string = "&".join([f"{key}={value}" for key, value in params.items()])
        url += f"?{query_string}"
        return url

    @staticmethod
    def _parse_response(response):
        """
        Parses the API response and handles potential errors.

        Args:
            response (requests.Response): The raw API response.

        Returns:
            dict or None: A dictionary containing the parsed data if successful,
                           or None if there was an error.
        """
        try:
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
            return None
        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f"Error Connecting: {conn_err}")
            return None
        except requests.exceptions.Timeout as timeout_err:
            logging.error(f"Timeout Error: {timeout_err}")
            return None
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Other error occurred: {req_err}")
            return None
        except json.JSONDecodeError as json_err:
            logging.error(f"JSON Decoding Error: {json_err}")
            logging.info(f"Response Content: {response.text}")
            return None

    def load_current_weather(self, lat, lon, meter, units="imperial"):
        """
        Gets the current weather for a given latitude and longitude.

        Args:
            lat (float): The latitude.
            lon (float): The longitude.
            units (str, optional): The unit system ('metric', 'imperial', or 'standard').
                                   Defaults to 'imperial'.

        Inserts the weather data into the 'weather_current' table.
        """
        conn = None
        cursor = None
        
        endpoint = "weather"
        params = {"lat": lat, "lon": lon, "units": units}
        url = self._build_url(endpoint, params)
        response = requests.get(url)
        response = WeatherAPIWrapper._parse_response(response)
        if response:
            try:
                conn = connect_to_database()
                if conn:
                    cursor = conn.cursor()
                    current_time = datetime.now()
                    data : dict = {
                        'collection_time': current_time,
                        'temperature': round(response['main']['temp']),
                        'temperature_min': round(response['main']['temp_min']),
                        'temperature_max': round(response['main']['temp_max']),
                        'humidity': response['main']['humidity'],
                        'description': response['weather'][0]['description'],
                        'feels_like': round(response['main']['feels_like']),
                        'wind_speed': decimal.Decimal(response['wind']['speed']),
                        'wind_direction': response['wind']['deg']
                    }
                    cursor.execute("""
                        INSERT INTO weather_current (collection_time, temperature, temperature_min, temperature_max, humidity, description, feels_like, wind_speed, wind_direction)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (collection_time) DO UPDATE SET
                            temperature = EXCLUDED.temperature,
                            temperature_min = EXCLUDED.temperature_min,
                            temperature_max = EXCLUDED.temperature_max,
                            humidity = EXCLUDED.humidity,
                            description = EXCLUDED.description,
                            feels_like = EXCLUDED.feels_like,
                            wind_speed = EXCLUDED.wind_speed,
                            wind_direction = EXCLUDED.wind_direction;
                    """, (data['collection_time'], data['temperature'], data['temperature_min'], data['temperature_max'], data['humidity'], data['description'], data['feels_like'], data['wind_speed'], data['wind_direction']))
                    conn.commit()
                    temperature_counter = meter.create_counter("temperature_readings", unit="F", description="Temperature readings")
                    temperature_counter.add(round(response['main']['temp']), attributes={"location": "home"})

                    logging.info("load_current_weather:  Successfully Loaded Current Weather")
            except psycopg2.Error as e:
                logging.error(f"load_current_weather: Error inserting/updating weather_current: {e}")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()

    def load_forecast(self, lat, lon, units="imperial"):
        """
        Gets the weather forecast for a given latitude and longitude.

        Note that this is the hourly forecast, but the results are displayed daily

        Args:
            lat (float): The latitude.
            lon (float): The longitude.
            units (str, optional): The unit system ('metric', 'imperial', or 'standard').
                                   Defaults to 'imperial'.

       Inserts the weather data into the 'weather_forecast' table.
        """
        conn = None
        cursor = None

        endpoint = "forecast"
        params = {"lat": lat, "lon": lon, "units": units}
        url = self._build_url(endpoint, params)
        response = requests.get(url)
        response = WeatherAPIWrapper._parse_response(response)

        if response:
            try:
                conn = connect_to_database()
                if conn:
                    cursor = conn.cursor()
                    current_time = datetime.now()
                    for item in response['list']:
                        forecast_date = date.fromtimestamp(item['dt'])
                        data = {
                            'collection_time': current_time,
                            'forecast_date': forecast_date,
                            'temperature_min': round(item['main']['temp_min']),
                            'temperature_max': round(item['main']['temp_max']),
                            'humidity_min': item['main']['humidity'],
                            'humidity_max': item['main']['humidity'],
                            'description': item['weather'][0]['description']
                        }
                        cursor.execute("""
                        INSERT INTO weather_forecast (collection_time, forecast_date, temperature_min, temperature_max, humidity_min, humidity_max, description)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                         ON CONFLICT (collection_time, forecast_date) DO UPDATE SET
                            temperature_min = EXCLUDED.temperature_min,
                            temperature_max = EXCLUDED.temperature_max,
                            humidity_min = EXCLUDED.humidity_min,
                            humidity_max = EXCLUDED.humidity_max,
                            description = EXCLUDED.description;
                        """, (data['collection_time'], data['forecast_date'], data['temperature_min'], data['temperature_max'], data['humidity_min'], data['humidity_max'], data['description']))
                        conn.commit()

                    logging.info("load_forecast: Successfully Loaded Forecast Weather")
            except psycopg2.Error as e:
                logging.error(f"load_forecast: Error inserting/updating weather_forecast: {e}")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()


def get_env_variable(var_name, default=None):
    """
    Gets an environment variable.

    Args:
        var_name (str): Name of the environment variable.
        default (any, optional): Default value if the variable is not found.

    Returns:
        any: The value of the environment variable, or the default if not found.

    Raises:
        ValueError: If the environment variable is not found and no default
                    value is provided.
    """
    value = os.environ.get(var_name)
    if value is None:
        if default is not None:
            return default
        else:
            raise ValueError(f"Environment variable '{var_name}' not set.")
    return value

def connect_to_database():
    """Connects to the PostgreSQL database."""
    db_host = get_env_variable("POSTGRES_HOST")
    db_port = get_env_variable("POSTGRES_PORT")
    db_name = get_env_variable("POSTGRES_DB")
    db_user = get_env_variable("POSTGRES_USER")
    db_password = get_env_variable("POSTGRES_PASSWORD")

    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        logging.info("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to the database: {e}")
        return None

def create_tables() -> bool:
    """
    Checks if the weather_current and weather_forecast tables exist in the
    database, and creates them if they do not.
    """
    success = True
    conn = connect_to_database()
    if not conn:
        logging.error("create_tables:  Database connection is not established.")
        return False

    cursor = conn.cursor()

    try:
        # Check for weather_current table
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE  table_name   = 'weather_current'
            );
        """)
        weather_current_exists = cursor.fetchone()[0]

        if not weather_current_exists:
            logging.info("Creating weather_current table.")
            cursor.execute("""
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
            """)
            conn.commit()

        # Check for weather_forecast table
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE  table_name   = 'weather_forecast'
            );
        """)
        weather_forecast_exists = cursor.fetchone()[0]

        if not weather_forecast_exists:
            logging.info("Creating weather_forecast table.")
            cursor.execute("""
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
            """)
            conn.commit()

    except psycopg2.Error as e:
        success = False
        logging.error(f"create_tables: Error creating tables: {e}")
    finally:
        cursor.close()

    if conn:
        conn.close()
    return success

def setup_opentelemetry():
    logging.info("Setting up OpenTelemetry")
    exporter = OTLPMetricExporter(endpoint="http://otel-collector.monitoring.svc.cluster.local:4317", insecure=True)
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=5000)
    provider = MeterProvider(metric_readers=[reader])
    metrics.set_meter_provider(provider)

    return metrics.get_meter("weather-app")

def main():
    """
    Main function to continuously retrieve and log weather information.

    Reads API key, latitude, longitude, and sleep time from environment variables.
    Calls the weather API repeatedly, pausing for the specified sleep time between calls.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    api_key = get_env_variable("OPENWEATHER_API_KEY")
    latitude = float(get_env_variable("LATITUDE"))
    longitude = float(get_env_variable("LONGITUDE"))
    total_sleep_time = float(get_env_variable("WEATHER_SLEEP_SECONDS"))
    weather_api = WeatherAPIWrapper(api_key)
    meter = setup_opentelemetry()

    logging.info('Starting Weather Collector')
    logging.info(f'latitude: {latitude}  longitude: {longitude}')

    tables_exist = create_tables()

    while tables_exist:
        start_time = datetime.now()
        logging.info(f'Running at: {start_time}')
        weather_api.load_current_weather(latitude, longitude, meter)
        weather_api.load_forecast(latitude, longitude)
        end_time = datetime.now()
        sleep_time = total_sleep_time - (end_time - start_time).total_seconds()
        sleep(sleep_time)


if __name__ == "__main__":
    main()
