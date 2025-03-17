from datetime import date, datetime
from time import sleep
import logging
import psycopg2
import os
import requests
import json


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

    def get_current_weather_by_coordinates(self, lat, lon, units="imperial"):
        """
        Gets the current weather for a given latitude and longitude.

        Args:
            lat (float): The latitude.
            lon (float): The longitude.
            units (str, optional): The unit system ('metric', 'imperial', or 'standard').
                                   Defaults to 'imperial'.

        Returns:
            dict or None: A dictionary containing the current weather data,
                           including temperature, humidity, description, etc.
                           Returns None if there was an error.
        """
        data = None
        endpoint = "weather"
        params = {"lat": lat, "lon": lon, "units": units}
        url = self._build_url(endpoint, params)
        response = requests.get(url)
        response = WeatherAPIWrapper._parse_response(response)
        if response:
            data = {
                'temperature': round(response['main']['temp']),
                'temperature_min': round(response['main']['temp_min']),
                'temperature_max': round(response['main']['temp_max']),
                'humidity': response['main']['humidity'],
                'description': response['weather'][0]['description'],
                'feels_like': round(response['main']['feels_like']),
                'wind_speed': response['wind']['speed'],
                'wind_direction': response['wind']['deg']
            }
        return data

    def get_forecast_by_coordinates(self, lat, lon, units="imperial"):
        """
        Gets the weather forecast for a given latitude and longitude.

        Note that this is the hourly forecast, but the results are displayed daily

        Args:
            lat (float): The latitude.
            lon (float): The longitude.
            units (str, optional): The unit system ('metric', 'imperial', or 'standard').
                                   Defaults to 'imperial'.

        Returns:
            dict or None: A dictionary where keys are dates and values are
                           dictionaries containing the daily forecast data
                           (min/max temperature, min/max humidity, description).
                           Returns None if there was an error.
        """
        data = None
        endpoint = "forecast"
        params = {"lat": lat, "lon": lon, "units": units}
        url = self._build_url(endpoint, params)
        response = requests.get(url)
        response = WeatherAPIWrapper._parse_response(response)

        if response and 'list' in response:
            data = {}
            for item in response['list']:
                weather_date = date.fromtimestamp(item['dt'])
                if not weather_date in data:
                    data[weather_date] = {
                        'temperature_min': round(item['main']['temp_min']),
                        'temperature_max': round(item['main']['temp_max']),
                        'humidity_min': item['main']['humidity'],
                        'humidity_max': item['main']['humidity'],
                        'description': item['weather'][0]['description']
                    }
                else:
                    if item['main']['temp_min'] < data[weather_date]['temperature_min']:
                        data[weather_date]['temperature_min'] = round(item['main']['temp_min'])
                    if item['main']['temp_max'] > data[weather_date]['temperature_max']:
                        data[weather_date]['temperature_max'] = round(item['main']['temp_max'])
                    if item['main']['humidity'] < data[weather_date]['humidity_min']:
                        data[weather_date]['humidity_min'] = item['main']['humidity']
                    if item['main']['humidity'] > data[weather_date]['humidity_max']:
                        data[weather_date]['humidity_max'] = item['main']['humidity']
                    if item['weather'][0]['description'] not in data[weather_date]['description']:
                        data[weather_date]['description'] += ", " + item['weather'][0]['description']
        return data


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


def call_api(weather_api: WeatherAPIWrapper, latitude: float, longitude: float):
    """
    Calls the weather API to retrieve and logs the current weather and forecast data.

    Args:
        weather_api (WeatherAPIWrapper): An instance of the WeatherAPIWrapper.
        latitude (float): The latitude for the weather data.
        longitude (float): The longitude for the weather data.
    """
    weather = weather_api.get_current_weather_by_coordinates(latitude, longitude)
    if weather:
        logging.info(f"Current: Temp:{weather['temperature']}  Minimum Temp: {weather['temperature_min']}  "
              f"Maximum Temp: {weather['temperature_max']} Humidity: {weather['humidity']}  "
              f"Description: {weather['description']}")
        logging.info(f"Feels Like: {weather['feels_like']}  Wind Speed: {weather['wind_speed']}  "
              f"Wind Direction: {weather['wind_direction']}")

    forecast = weather_api.get_forecast_by_coordinates(latitude, longitude)
    if forecast:
        for day, weather in forecast.items():
            logging.info(
                f"Date: {day}  Minimum Temp: {weather['temperature_min']}  Maximum Temp: {weather['temperature_max']} "
                f"Minimum Humidity: {weather['humidity_min']}  Maximum Humidity: {weather['humidity_max']} "
                f"Description: {weather['description']}")


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

    logging.info('Starting Weather Collector')
    logging.info(f'latitude: {latitude}  longitude: {longitude}')

    tables_exist = create_tables()

    while tables_exist:
        start_time = datetime.now()
        logging.info(f'Running at: {start_time}')
        conn = connect_to_database()
        if conn:
            call_api(weather_api, latitude, longitude)
            conn.close()
        else:
            logging.error('Could not connect to database')
        end_time = datetime.now()
        sleep_time = total_sleep_time - (end_time - start_time).total_seconds()
        sleep(sleep_time)


if __name__ == "__main__":
    main()
