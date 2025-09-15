import decimal
import os
import psycopg2
import requests
import traceback
from datetime import date, datetime
from dotenv import load_dotenv
from jTookkit.jLogging import LoggingInfo, Logger, EventType
from jTookkit.jConfig import Config


class WeatherCollector:
    """
    A wrapper class for interacting with a weather API, specifically OpenWeatherMap.

    This class provides methods to retrieve current weather conditions and
    forecast information based on latitude and longitude coordinates.
    """

    def __init__(self, config:Config):
        """
        Initializes the WeatherAPIWrapper.

        Args:
            config (dict): configuration as a dictionary
        """
        self._config = config
        self.api_key = get_env_variable("OPENWEATHER_API_KEY")
        self.base_url = get_env_variable("BASE_URL")
        logging_info = LoggingInfo(**self._config.get("logging_info", {}))
        self._logger = Logger(logging_info)
        self._transaction = None

    def process(self):
        payload= {}
        overall_return_code = 200

        self._transaction = self._logger.transaction_event(EventType.TRANSACTION_START)
        try:
            latitude = float(get_env_variable("LATITUDE"))
            longitude = float(get_env_variable("LONGITUDE"))
            if get_env_variable("CREATE_TABLE", "false") == "true":
                return_code = self._create_tables()
                if return_code > overall_return_code:
                    overall_return_code = return_code

            # Get and Load Current Weather
            if overall_return_code == 200:
                return_code, results = self._get_weather(latitude, longitude, lookup_type="Current")
                if return_code > overall_return_code:
                    overall_return_code = return_code
                else:
                    return_code = self._load_current_weather(results)
                    if return_code > overall_return_code:
                        overall_return_code = return_code
                        payload['current'] = len(results)
            # Get and Load Forecast
            if overall_return_code == 200:
                return_code, results = self._get_weather(latitude, longitude, lookup_type="Forecast")
                if return_code > overall_return_code:
                    overall_return_code = return_code
                else:
                    return_code = self._load_forecast(results)
                    if return_code > overall_return_code:
                        overall_return_code = return_code
                        payload['forecast'] = len(results)
        except Exception as ex:
            overall_return_code = 500
            stack_trace = traceback.format_exc()
            message = "Exception in process"
            payload['message'] = message
            self._logger.message(message=message, exception=ex, stack_trace=stack_trace, transaction=self._transaction)
        self._logger.transaction_event(EventType.TRANSACTION_END, transaction=self._transaction,
                                       payload=payload, return_code=overall_return_code)


    def _get_weather(self, latitude, longitude, lookup_type="Current", units="imperial"):
        """
        Gets the current or forecast weather for a given latitude and longitude.

        Args:
            latitude (float): The latitude.
            longitude (float): The longitude.
            units (str, optional): The unit system ('metric', 'imperial', or 'standard').
                                   Defaults to 'imperial'.
        """
        results = None
        return_code = 200
        payload = {}
        source_transaction = self._logger.transaction_event(EventType.SPAN_START,
                                                            source_component=f"OpenWeather: {lookup_type}",
                                                            transaction=self._transaction)
        try:
            if lookup_type == "Current":
                url = f"{self.base_url}weather"
            else:
                url = f"{self.base_url}forecast"
            params = {"lat": latitude, "lon": longitude, "units": units, "appid": self.api_key}
            query_string = "&".join([f"{key}={value}" for key, value in params.items()])
            url += f"?{query_string}"
            response = requests.get(url)
            response.raise_for_status()
            results = response.json()
        except Exception as ex:
            return_code = 500
            stack_trace = traceback.format_exc()
            message = f"Exception calling OpenWeather {lookup_type}"
            payload['message'] = message
            self._logger.message(message=message, exception=ex, stack_trace=stack_trace, transaction=source_transaction)
        self._logger.transaction_event(EventType.SPAN_END, transaction=source_transaction, payload=payload,
                                       return_code=return_code)
        return return_code, results


    def _load_current_weather(self, results):
        """
        Inserts the weather data into the 'weather_current' table.
        """
        conn = None
        cursor = None
        return_code = 200
        payload = {}
        source_transaction = self._logger.transaction_event(EventType.SPAN_START,
                                                            source_component="Postgres: weather_current",
                                                            transaction=self._transaction)
        try:
            conn = connect_to_database()
            cursor = conn.cursor()
            current_time = datetime.now()
            temp = round(results['main']['temp'])
            data : dict = {
                'collection_time': current_time,
                'temperature': temp,
                'temperature_min': round(results['main']['temp_min']),
                'temperature_max': round(results['main']['temp_max']),
                'humidity': results['main']['humidity'],
                'description': results['weather'][0]['description'],
                'feels_like': round(results['main']['feels_like']),
                'wind_speed': decimal.Decimal(results['wind']['speed']),
                'wind_direction': results['wind']['deg']
            }
            cursor.execute("""
                INSERT INTO weather_current (collection_time, temperature, temperature_min, temperature_max, 
                humidity, description, feels_like, wind_speed, wind_direction)
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
            """, (data['collection_time'], data['temperature'], data['temperature_min'],
                  data['temperature_max'], data['humidity'], data['description'], data['feels_like'],
                  data['wind_speed'], data['wind_direction']))
            conn.commit()
        except Exception as ex:
            return_code = 500
            stack_trace = traceback.format_exc()
            message = "Exception loading weather_current"
            payload['message'] = message
            self._logger.message(message=message, exception=ex, stack_trace=stack_trace,
                                 transaction=source_transaction)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        self._logger.transaction_event(EventType.SPAN_END, transaction=source_transaction, payload=payload,
                                       return_code=return_code)
        return return_code

    def _load_forecast(self, results):
        """
        Inserts the weather data into the 'weather_current' table.
        """
        conn = None
        cursor = None
        return_code = 200
        payload = {}
        source_transaction = self._logger.transaction_event(EventType.SPAN_START,
                                                            source_component="Postgres: weather_current",
                                                            transaction=self._transaction)
        try:
            conn = connect_to_database()
            cursor = conn.cursor()
            current_time = datetime.now()
            for item in results['list']:
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
                INSERT INTO weather_forecast (collection_time, forecast_date, temperature_min, temperature_max, 
                humidity_min, humidity_max, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                 ON CONFLICT (collection_time, forecast_date) DO UPDATE SET
                    temperature_min = EXCLUDED.temperature_min,
                    temperature_max = EXCLUDED.temperature_max,
                    humidity_min = EXCLUDED.humidity_min,
                    humidity_max = EXCLUDED.humidity_max,
                    description = EXCLUDED.description;
                """, (data['collection_time'], data['forecast_date'], data['temperature_min'],
                      data['temperature_max'], data['humidity_min'], data['humidity_max'], data['description']))
                conn.commit()
        except Exception as ex:
            return_code = 500
            stack_trace = traceback.format_exc()
            message = "Exception loading weather_forecast"
            payload['message'] = message
            self._logger.message(message=message, exception=ex, stack_trace=stack_trace,
                                 transaction=source_transaction)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        self._logger.transaction_event(EventType.SPAN_END, transaction=source_transaction, payload=payload,
                                       return_code=return_code)
        return return_code


    def _create_tables(self):
        """
        Checks if the weather_current and weather_forecast tables exist in the
        database, and creates them if they do not.
        """
        return_code = 200
        payload = {}
        conn = None
        cursor = None
        source_transaction = self._logger.transaction_event(EventType.SPAN_START,
                                                            source_component="Postgres: Create Tables",
                                                            transaction=self._transaction)
        try:
            conn = connect_to_database()
            cursor = conn.cursor()
            self._create_weather_current_table(conn, cursor, source_transaction)
            self._create_weather_forecast_table(conn, cursor, source_transaction)
        except Exception as ex:
            return_code = 500
            stack_trace = traceback.format_exc()
            message = "Exception creating tables"
            payload['message'] = message
            self._logger.message(message=message, exception=ex, stack_trace=stack_trace,
                                 transaction=source_transaction)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        self._logger.transaction_event(EventType.SPAN_END, transaction=source_transaction, payload=payload,
                                       return_code=return_code)
        return return_code

    def _create_weather_current_table(self, conn, cursor, source_transaction):
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE  table_name   = 'weather_current'
            );
        """)
        weather_current_exists = cursor.fetchone()[0]
        if not weather_current_exists:
            self._logger.message(source_transaction, message="Creating weather_current table", debug=True)
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

    def _create_weather_forecast_table(self, conn, cursor, source_transaction):
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE  table_name   = 'weather_forecast'
            );
        """)
        weather_forecast_exists = cursor.fetchone()[0]
        if not weather_forecast_exists:
            self._logger.message(source_transaction, message="Creating weather_forecast table", debug=True)
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
    """Connects to the Postgres database."""
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password,
    )
    return conn


def main():
    load_dotenv()
    config = Config()
    collector = WeatherCollector(config)
    collector.process()

if __name__ == "__main__":
    main()
