from __future__ import annotations
import pendulum 

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import requests
import cdsapi
import os
import xarray as xr
import pandas as pd
import random
import numpy as np

def find_latest_available_date(variable='2m_temperature', area=[40.21, -1.52, 38.69, -0.03]):
    """
    Busca hacia atrás la fecha más reciente con datos a las 23:00 UTC,
    y luego descarga todo el día (00:00 a 23:00).
    """
    import os
    print("PERVIO AL LOGIN")
    c = cdsapi.Client(
        url="https://cds.climate.copernicus.eu/api",
        key="26032064-cdae-4843-9bbb-c6e11f89149c"
    )
    print("POST LOGIN")
    
    now = pendulum.now("UTC")
    
    # Paso 1: buscar el último día con datos a las 23:00
    for delta_days in range(0, 10):
        date = now.subtract(days=delta_days)
        year = date.year
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        time_str = "23:00"
        
        try:
            print(f"Comprobando datos para {year}-{month}-{day} {time_str} ...")
            # Solo un "dry run" para comprobar si hay datos
            c.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'variable': variable,
                    'year': str(year),
                    'month': month,
                    'day': day,
                    'time': time_str,
                    'format': 'netcdf',
                    "area": area
                },
                'dummy.nc'
            )
            print(f"Datos encontrados para {year}-{month}-{day}, descargando día completo...")
            
            # Paso 2: descargar todo el día
            hours = [f"{h:02d}:00" for h in range(24)]
            output_file = f"era5_{year}{month}{day}.nc"
            
            c.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'variable': variable,
                    'year': str(year),
                    'month': month,
                    'day': day,
                    'time': hours,
                    'format': 'netcdf',
                    "area": area
                },
                output_file
            )
            print(f"Datos descargados en {output_file}")
            # eliminar dummy si existe
            if os.path.exists("dummy.nc"):
                os.remove("dummy.nc")
            
            return output_file  # devolver el archivo descargado
        
        except Exception as e:
            print(f"No hay datos para {year}-{month}-{day}, intentando día anterior...")
            continue
    
    raise ValueError("No se encontraron datos en los últimos 10 días")

def retrieve_and_process_data():
    """
    Function to process the data.
    """

    file = find_latest_available_date()
    
    # === Step 2: Extract a value ===
    ds = xr.open_dataset(file)

    # Extraer latitudes y longitudes
    lats = ds['latitude'].values
    lons = ds['longitude'].values
    temps = ds['t2m'].values  # shape: [time, lat, lon]
    time_values = ds["valid_time"].values
    # print(time_values.shape)
    # print(time_values)

    # Convertimos a °C
    temps_c = temps - 273.15
    #print(":::::::::")
    # Crear lista de diccionarios con coordenadas y temperatura
    temperature_points = []
    for k, time_val in enumerate(time_values):
        # Convert the time_val from a NumPy datetime64 object to a pandas Timestamp
        timestamp = pd.to_datetime(time_val)
        # Extract the hour from the Timestamp
        hour_str = f"{timestamp.hour:02d}:00"

        for i, lat in enumerate(lats):
            for j, lon in enumerate(lons):
                # Check if there is data for the specific time step
                if k < temps_c.shape[0]:
                    temperature_points.append({
                        "lat": float(lat),
                        "lon": float(lon),
                        "temperature": np.round(float(temps_c[k, i, j]),2),
                        "hour": hour_str
                    })

    sample_size = 10  # número de puntos aleatorios a mostrar
    temperature_sample = random.sample(temperature_points, sample_size)

    print("---------> Temperature points sample (aleatorio):")
    for point in temperature_sample:
        print(point)

with DAG(
    dag_id="copernicus",
    start_date=pendulum.datetime(2025, 9, 4, tz="UTC"),
    schedule="*/10 * * * *",  # Cron expression for every 10 minutes
    catchup=False,
    tags=["api", "data"],
) as dag:
    call_api_task = PythonOperator(
        task_id="call_and_process_api",
        python_callable=retrieve_and_process_data,
    )
 