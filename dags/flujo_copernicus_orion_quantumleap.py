from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import requests
import cdsapi
import xarray as xr
import pandas as pd
import numpy as np
import random
import json

# ===== Config IoT / Orion =====
IOTA_NORTH_URL = "http://iot-agent:4041"   # Registro de dispositivos
IOTA_SOUTH_URL = "http://iot-agent:7896"   # Ingestión de datos
ORION_URL_V2 = "http://orion-ld:1026/v2"   # NGSIv2 para QuantumLeap
QUANTUMLEAP_URL = "http://quantumleap:8668"  # QuantumLeap

FIWARE_SERVICE = "openiot"
FIWARE_SERVICEPATH = "/"
DEVICE_ID = "sensor002"
ENTITY_ID = "sensor002"
API_KEY = "123456"
area_VLC = [40.21, -1.52, 38.69, -0.03]

# ========= Funciones ========= #

def download_data(**context):
    c = cdsapi.Client(url="https://cds.climate.copernicus.eu/api",
                      key="26032064-cdae-4843-9bbb-c6e11f89149c")
    now = pendulum.now("UTC")
    for delta_days in range(0, 10):
        date = now.subtract(days=delta_days)
        year = date.year
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"

        try:
            hours = [f"{h:02d}:00" for h in range(24)]
            output_file = f"/tmp/era5_{year}{month}{day}.nc"
            c.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'variable': "2m_temperature",
                    'year': str(year),
                    'month': month,
                    'day': day,
                    'time': hours,
                    'format': 'netcdf',
                    "area": area_VLC
                },
                output_file
            )
            context["ti"].xcom_push(key="data_file", value=output_file)
            return
        except Exception:
            continue
    raise ValueError("No se encontraron datos en los últimos 10 días")

def parse_data(**context):
    file = context["ti"].xcom_pull(key="data_file", task_ids="download_data")
    ds = xr.open_dataset(file)
    lats = ds['latitude'].values
    lons = ds['longitude'].values
    temps = ds['t2m'].values
    time_values = ds["valid_time"].values
    temps_c = temps - 273.15

    temperature_points = []
    for k, time_val in enumerate(time_values):
        timestamp = pd.to_datetime(time_val)
        hour_str = f"{timestamp.hour:02d}:00"

        for i, lat in enumerate(lats):
            for j, lon in enumerate(lons):
                if k < temps_c.shape[0]:
                    temperature_points.append({
                        "lat": float(lat),
                        "lon": float(lon),
                        "temperature": np.round(float(temps_c[k, i, j]), 2),
                        "hour": hour_str
                    })

    sample = random.sample(temperature_points, 5)
    print("Sample points:", sample)
    context["ti"].xcom_push(key="temperature_points", value=temperature_points)

def register_device(**_):
    headers = {
        "Content-Type": "application/json",
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH
    }

    device_check = requests.get(f"{IOTA_NORTH_URL}/iot/devices/{DEVICE_ID}", headers=headers)
    if device_check.status_code == 404:
        print(f"Registering device {DEVICE_ID}...")
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
        print(f"Register response: {r.status_code} {r.text}")
    else:
        print(f"Device {DEVICE_ID} already exists.")

def send_data(**context):
    temperature_points = context["ti"].xcom_pull(key="temperature_points", task_ids="parse_data")
    headers = {
        "Content-Type": "text/plain",
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH
    }
    for point in temperature_points:
        data_payload = f"t|{point['temperature']}|lat|{point['lat']}|lon|{point['lon']}|h|{point['hour']}"
        r = requests.post(f"{IOTA_SOUTH_URL}/iot/d?i={DEVICE_ID}&k={API_KEY}",
                          data=data_payload, headers=headers)
        print(f"Sent: {data_payload}, status {r.status_code}")

def check_orion(**_):
    headers = {"Fiware-Service": FIWARE_SERVICE, "Fiware-ServicePath": FIWARE_SERVICEPATH}
    r = requests.get(f"{ORION_URL_V2}/entities/{ENTITY_ID}", headers=headers)
    print("Orion entity:", r.status_code, r.text)

def ensure_subscription(**_):
    headers = {
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH,
        "Content-Type": "application/json"
    }

    # Obtener todas las suscripciones
    r = requests.get(f"{ORION_URL_V2}/subscriptions", headers=headers)
    subs = r.json() if r.status_code == 200 else []

    # Comprobar si ya existe una suscripción para esta entidad y URL
    for sub in subs:
        entities = sub.get("subject", {}).get("entities", [])
        print("Entities: ",entities)
        notification_url = sub.get("notification", {}).get("http", {}).get("url")
        print("notification url: ",notification_url)
        if any(e.get("id") == ENTITY_ID for e in entities) and notification_url == f"{QUANTUMLEAP_URL}/v2/notify":
            print("Subscription already exists:", sub.get("id"))
            return

    # Crear la suscripción si no existe
    payload = {
        "description": "Subscription for sensor002 to QuantumLeap",
        "subject": {"entities": [{"id": ENTITY_ID, "type": "Device"}]},
        "notification": {"http": {"url": f"{QUANTUMLEAP_URL}/v2/notify"}},
        "throttling": 1
    }

    r = requests.post(f"{ORION_URL_V2}/subscriptions", headers=headers, json=payload)
    print("Subscription creation:", r.status_code, r.text)

def check_quantumleap(**_):
    q = "SELECT * FROM doc.etdevice WHERE entity_id = 'sensor002' ORDER BY time_index DESC LIMIT 1;"
    r = requests.post("http://crate-db:4200/_sql", json={"stmt": q})  # usa crate-db si estás en Docker
    if r.status_code == 200:
        rows = r.json()["rows"]
        if rows:
            print("CrateDB check OK. Latest row:", rows[0])
        else:
            print("CrateDB check: no data found.")
    else:
        print("CrateDB check failed:", r.status_code, r.text)

# ========= DAG ========= #
with DAG(
    dag_id="flujo_copernicus_orion_modular",
    start_date=pendulum.datetime(2025, 9, 4, tz="UTC"),
    schedule="0 * * * *",
    catchup=False,
    tags=["api", "data", "orion", "iot-agent"],
) as dag:

    t1 = PythonOperator(task_id="download_data", python_callable=download_data)
    t2 = PythonOperator(task_id="parse_data", python_callable=parse_data)
    t3 = PythonOperator(task_id="register_device", python_callable=register_device)
    t4 = PythonOperator(task_id="send_data", python_callable=send_data)
    t5 = PythonOperator(task_id="check_orion", python_callable=check_orion)
    t6 = PythonOperator(task_id="ensure_subscription", python_callable=ensure_subscription)
    t7 = PythonOperator(task_id="check_quantumleap", python_callable=check_quantumleap)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
