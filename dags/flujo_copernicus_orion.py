from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import requests
import cdsapi
import xarray as xr
import time
import os
import pandas as pd
import random
import numpy as np

# ===== Config IoT / Orion =====
IOTA_NORTH_URL = "http://iot-agent:4041"   # Registro de servicios y dispositivos
IOTA_SOUTH_URL = "http://iot-agent:7896"   # Ingestión de datos
### QUE LIO DE URLS
ORION_URL = "http://orion-ld:1026/ngsi-ld/v1/entities" # para conectar con iot agent uso ngsi -v2
ORION_URL_V2 = "http://orion-ld:1026/v2" # para conectar con quantum leap uso ngsi-ld (v1)
### FIN LIO URLS ORION
FIWARE_SERVICE = "openiot"
FIWARE_SERVICEPATH = "/"
DEVICE_ID = "sensor002"
ENTITY_ID = "urn:ngsi-ld:Device:002"
API_KEY = "123456"  # Ajustar a la clave real generada por IoT Agent
area_VLC =[40.21, -1.52, 38.69, -0.03]

def find_latest_available_date(variable='2m_temperature', area=area_VLC):
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

def Quantum_Leap_ensure_subscription_exists():
    """
    Crea la suscripción en Orion-LD hacia QuantumLeap si no existe.
    """
    headers_base = {
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH
    }

    headers_json = {
        **headers_base,
        "Content-Type": "application/json"
    }
    subscription_id = "sub-ql-sensor002"

    # 1. Verificar si la suscripción ya existe
    r = requests.get(f"{ORION_URL_V2}/subscriptions", headers=headers_base)
    if r.status_code == 200:
        existing_subs = r.json()
        if any(sub.get("id") == subscription_id for sub in existing_subs):
            print(f"---------> Subscription {subscription_id} already exists in Orion.")
            return
    else:
        print(f"---------> Could not fetch subscriptions: {r.status_code} {r.text}")

    # 2. Crear la suscripción en formato NGSI-v2 ya que tengo el IotAgent en v2
    subscription_payload = {
        "description": "Subscription for sensor002 to QuantumLeap",
        "subject": {
            "entities": [
                {"id": DEVICE_ID, "type": "Device"}
            ],
            "condition": {
                "attrs": ["temperature", "lat", "lon", "hour"]
            }
        },
        "notification": {
            "http": {
                "url": "http://quantumleap:8668/v2/notify"
            },
            "attrs": ["temperature", "lat", "lon", "hour"]
        },
        "throttling": 1
    }

    r = requests.post(f"{ORION_URL_V2}/subscriptions",
                  headers=headers_json,
                  json=subscription_payload)

    if r.status_code in [201, 204]:
        print(f"---------> Subscription {subscription_id} created successfully.")
    else:
        print(f"---------> Error creating subscription: {r.status_code}, {r.text}")
        
    # 3. Check simple de datos en QuantumLeap
    print("---------> Checking data in QuantumLeap...")
    ql_url = f"http://quantumleap:8668/v2/entities/{DEVICE_ID}?type=Device&options=keyValues"
    for attempt in range(5):
        try:
            r_ql = requests.get(ql_url, headers=headers_base, timeout=5)
            if r_ql.status_code == 200:
                data = r_ql.json()
                # Mostrar resumen limpio de los últimos datos
                print("---------> Latest data in QuantumLeap:")
                print(f"Temperature: {data.get('temperature')}")
                print(f"Latitude: {data.get('lat')}")
                print(f"Longitude: {data.get('lon')}")
                print(f"Hour: {data.get('hour')}")
                return
            else:
                print(f"Attempt {attempt+1}: {r_ql.status_code} {r_ql.text}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1}: Exception {e}")
        time.sleep(2)

    print("---------> No data found in QuantumLeap yet.")


def retrieve_and_send_to_orion():
    # === Step 1: Download data ===
    # print("---------> Downloading data from CDS...")
    # c = cdsapi.Client(
    #     url="https://cds.climate.copernicus.eu/api",
    #     key="26032064-cdae-4843-9bbb-c6e11f89149c"
    # )
    # c.retrieve(
    #     'reanalysis-era5-single-levels',
    #     {
    #         'product_type': 'reanalysis',
    #         'variable': '2m_temperature',
    #         'year': '2025',
    #         'month': '08',
    #         'day': '28',
    #         'time': '23:00',
    #         'format': 'netcdf',
    #         "area": [40.21, -1.52, 38.69, -0.03]
    #     },
    #     'output.nc'
    # )

    # === Step 2: Extract value ===

    file = find_latest_available_date("2m_temperature",area_VLC)

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

    headers = {
        "Content-Type": "application/json",
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH
    }

    # === Step 3: Register device if not exists ===
    device_check = requests.get(f"{IOTA_NORTH_URL}/iot/devices/{DEVICE_ID}", headers=headers)
    if device_check.status_code == 404:
        print(f" ---------> Device {DEVICE_ID} not found. Registering...")

        device_payload = {
            "devices": [
                {
                    "device_id": DEVICE_ID,
                    "entity_name": ENTITY_ID,
                    "entity_type": "Device",
                    "apikey": API_KEY,
                    "protocol": "PDI-IoTA-UltraLight",
                    "transport": "HTTP",
                    "attributes": [
                        {"object_id": "t", "name": "temperature", "type": "Number"},
                        {"object_id": "lat", "name": "lat", "type": "Number"},
                        {"object_id": "lon", "name": "lon", "type": "Number"},
                        {"object_id": "h", "name": "hour", "type": "Text"}
                    ]
                }
            ]
        }

        r = requests.post(f"{IOTA_NORTH_URL}/iot/devices", json=device_payload, headers=headers)
        if r.status_code in [201, 409]:
            print(f"---------> Device {DEVICE_ID} registered successfully or already exists.")
        else:
            print(f"---------> Error registering device: {r.status_code}, {r.text}")

        time.sleep(2)  # Esperamos 2 segundos para que IoT Agent cree la entidad
    else:
        print(f"---------> Device {DEVICE_ID} already exists.")


    # === Step 4: Send value to IoT Agent ===
    # Construir payload con todos los puntos
    headers_for_data = {
        "Content-Type": "text/plain",
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH
    }

    for point in temperature_points:
        data_payload = f"t|{point['temperature']}|lat|{point['lat']}|lon|{point['lon']}|h|{point['hour']}"

        try:
            r = requests.post(
                f"{IOTA_SOUTH_URL}/iot/d?i={DEVICE_ID}&k={API_KEY}",
                data=data_payload,
                headers=headers_for_data
            )
            r.raise_for_status()
            print(f"---------> Sent: {data_payload}")
        except requests.RequestException as e:
            print(f"---------> Error sending data to IoT Agent: {e}")
            
    # try:
    #     r = requests.post(
    #         f"{IOTA_SOUTH_URL}/iot/d?i={DEVICE_ID}&k={API_KEY}",
    #         data="t|ads",
    #         headers=headers_for_data
    #     )
    #     r.raise_for_status()
    # except requests.RequestException as e:
    #     print(f"---------> Error sending data to IoT Agent: {e}")
        

    # === Step 5: Check entity in Orion-LD ===
    try:
        r = requests.get(f"{ORION_URL}/{ENTITY_ID}", headers=headers)
        r.raise_for_status()
        print(f"---------> Entity in Orion-LD:\n{r.json()}")
    except requests.RequestException as e:
        print(f"---------> Error fetching entity from Orion-LD: {e}")

    # ===  Step 6: Save values into Quantum Leap Database
    Quantum_Leap_ensure_subscription_exists()
    print(f"---------> DATA HAS BEEN SAVED INTO QUANTUM LEAP DATABASE")


with DAG(
    dag_id="flujo_copernicus_orion",
    start_date=pendulum.datetime(2025, 9, 4, tz="UTC"),
        schedule="0 * * * *",  # cada hora  Si usamos "*/10 * * * *" sería cada 10 minutos
    catchup=False,
    tags=["api", "data", "orion", "iot-agent"],
) as dag:
    call_api_task = PythonOperator(
        task_id="call_and_process_api",
        python_callable=retrieve_and_send_to_orion,
    )
