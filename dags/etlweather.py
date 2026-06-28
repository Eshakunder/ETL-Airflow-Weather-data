from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
#HTTPHook is a helper class that allows you to interact with HTTP endpoints. It provides methods for making GET, POST, PUT, and DELETE requests, as well as handling authentication and connection management.
# In order to push the data into database like mongodb or postgre there will be a need to use the respective hooks for those databases. For example, if you want to push the data into MongoDB, you can use the MongoHook provided by Airflow. Similarly, for PostgreSQL, you can use the PostgresHook.

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
import json
import requests


#Latitude and Longitude of the city for which we want to get the weather data
LATITUDE = '51.5074' # Example: New York City latitude
LONGITUDE = '-0.1278'  # Example: New York City longitude

#for our convenience , we can define the connection IDs.
POSTGRES_CONN_ID = 'postgres_default'  # Connection ID for PostgreSQL
API_CONN_ID = 'open_meteo_api'  # Connection ID for OpenWeatherMap API

#basic default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

#create a DAG instance

with DAG(dag_id = 'weather_etl_pipeline',
         default_args = default_args ,
         schedule = '@daily',         # renamed from schedule_interval in Airflow 2.4+
         catchup = False
         ) as dags:
    
    @task()
    def extract_weather_data():
        """Extract weather data from OpenWeatherMap API using Airflow connection."""

        #use HTTP Hook to get connection details from Airflow connection.
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        #Build the API endpoint .
        ##http://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'


        ##Make the request via the HTTP Hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()  # Return the JSON response if successful
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task()                                          # fixed: was indented inside extract_weather_data
    def transform_weather_data(weather_data):
        """Transform the extracted weather data into a format suitable for database insertion."""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
            'time': current_weather['time']         
        }
        return transformed_data
    
    @task()                                          # fixed: was indented inside extract_weather_data
    def load_weather_data(transformed_data):
        """Load the transformed weather data into the PostgreSQL database."""
        # Implementation for loading data into PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        con = pg_hook.get_conn()
        cursor = con.cursor()

        # Create table if it doesn't exist
        cursor.execute(""" 
                CREATE TABLE IF NOT EXISTS weather_data (
                    latitude FLOAT,
                    longitude FLOAT,
                    temperature FLOAT,
                    windspeed FLOAT,
                    winddirection FLOAT,
                    weathercode INT,
                    time TIMESTAMP
                );
        """)

        # Insert the transformed data into the table
        cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode, time)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode'],
            transformed_data['time']
        ))     

        con.commit()   # fixed: was cursor.commit(), should be con.commit()
        cursor.close()  # Close the cursor

    #dag workflow - etl pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)