from __future__ import annotations
 
import pendulum
import requests
from airflow.decorators import dag, task
import os 
@dag(
    dag_id='AEMET_Valencia_weather',
    start_date=pendulum.datetime(2025, 9, 3, tz='UTC'),
    schedule='*/10 * * * *',
    catchup=False,
    tags=['API', 'weather'],
)
def api_valencia_weather_dag():
    @task
    def get_weather_data():
        print("EL DIRECTORIO ES \n", os.getcwd())
        url = 'https://opendata.aemet.es/opendata/api/prediccion/ccaa/hoy/val'
        params = {
            'api_key': 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqbW9ybWFsQHVwdi5lZHUuZXMiLCJqdGkiOiI4ODU3YmFkZC02NzA4LTQ1ZTEtYTBhYi0xNWU4MmM3NjAxYzYiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc1MzQ0NTA2NCwidXNlcklkIjoiODg1N2JhZGQtNjcwOC00NWUxLWEwYWItMTVlODJjNzYwMWM2Iiwicm9sZSI6IiJ9.uLbdiS1IiSiWfghNyM_Ze0--Jd2_mR6JGi2hawgrkA0',
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            print(f'Error al llamar a la API: {e}')
            return None
 
    @task
    def process_data(data):
        print("the data is :", data)
        if data:
            url_data = data.get('datos')
 
            if url_data:
                try:
                    import json
                    print("the url to get the data is :", url_data)
                    response = requests.get(url_data)
                    print("HTTP status code:", response.status_code)
                    response.raise_for_status()
                    print(response.__dict__)
                    avisos_data = response.text
                    print(avisos_data)
                    return avisos_data
                except requests.exceptions.RequestException as e:
                    print(f'Error al obtener los datos de avisos: {e}')
                    return 'Error al procesar los datos'
            
        else:
            print('No se recibieron datos para procesar')
            return 'No se procesaron datos'
 
    t1 = get_weather_data()
    t2 = process_data(t1)
 
 
 
api_valencia_weather_dag()
 