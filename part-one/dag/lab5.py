from datetime import datetime, timedelta
import requests
from airflow.hooks.base import BaseHook
from airflow.sensors.http_sensor import HttpSensor
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


API_CONN_ID = 'openweather_api' # Connection ID for OpenWeather API
POSTGRES_CONN_ID = 'postgres_conn_id' # Connection ID for PostgreSQL

default_args = {
    'owner': 'Marzuk',
    'email': ['marzuk.entsie@amalitech.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}  # Default arguments for the DAG so that we don't have to repeat them

weather_connection = BaseHook.get_connection(API_CONN_ID) # Get the connection to OpenWeather API
api_key = weather_connection.extra_dejson.get('api_key') # Get the API key from the connection above

with DAG(
    'lab5', 
    default_args=default_args, 
    schedule_interval=None, 
    start_date=datetime(2024, 11, 28),
    catchup=False,
    tags=['labs']
) as dag: # Define the DAG

    # Task 1
    check_weather_api_task = HttpSensor(
        task_id='check_weather_api_sensor',
        http_conn_id=API_CONN_ID,
        endpoint=f'data/2.5/weather?q=Portland&APPID={api_key}',
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    ) # Check if the OpenWeather API is available

    # Fetch weather data for Portland
    def fetch_weather():
        url = f'https://api.openweathermap.org/data/2.5/weather?q=Portland&APPID={api_key}'
        response = requests.get(url)
        if response.status_code == 200:
            weather = response.json() # Get the weather data from the response and parse it as JSON
            return weather
        else:
            raise Exception(f'Failed to fetch weather. Status code: {response.status_code}')
    
    # Task 2
    fetch_weather_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather,
    )

    # Transform weather data
    def transform_weather(ti):
        weather = ti.xcom_pull(task_ids='fetch_weather') # Get the weather data from the previous task using XCom 
        # Convert temperature from Kelvin to Fahrenheit
        temp_kelvin = weather['main']['temp']
        temp_fahrenheit = (temp_kelvin - 273.15) * 9/5 + 32
        pressure = weather['main']['pressure']
        humidity = weather['main']['humidity']
        from datetime import timezone
        timestamp = datetime.fromtimestamp(weather['dt'], timezone.utc) # Convert timestamp to datetime object
        city = weather['name']
        
        return {
            'temp_fahrenheit': temp_fahrenheit,
            'pressure': pressure,
            'humidity': humidity,
            'timestamp': timestamp,
            'city': city
        }
    
    # Task 3
    transform_weather_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather,
    )

    # Save weather data to Postgres
    def load_weather_data(ti):
        """Load transformed data into PostgreSQL."""
        transformed_data = ti.xcom_pull(task_ids='transform_weather')
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        # Create Schema 
        cursor.execute('CREATE SCHEMA IF NOT EXISTS labs;')
        # Create Table 
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS labs.weather (
                temp_fahrenheit FLOAT,
                pressure INT,
                humidity INT,
                timestamp TIMESTAMP,
                city VARCHAR(255)
            );
        ''')

        # Insert data
        cursor.execute('''
            INSERT INTO labs.weather (temp_fahrenheit, pressure, humidity, timestamp, city)
            VALUES (%s, %s, %s, %s, %s);
        ''', (transformed_data['temp_fahrenheit'], transformed_data['pressure'], transformed_data['humidity'], transformed_data['timestamp'], transformed_data['city']))
        conn.commit()
        cursor.close()
    
    # Task 4
    load_weather_data_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data,
    )
        
    # Define the task dependencies
    check_weather_api_task >> fetch_weather_task >> transform_weather_task >> load_weather_data_task