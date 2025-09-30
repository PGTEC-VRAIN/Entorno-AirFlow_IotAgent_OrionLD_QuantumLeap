from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import re
import json
import pandas as pd
import os

def scrap_avamet_csv():
    '''
    Descarga los datos de la AVAMET utilizando técnicas de web scrapping.

    Dado que los datos se encuentran en la página web en formato JSON, se accede a ellos por web scrapping a través de las etiquetas html
    '''
    # --- Scraping ---
    url = "https://www.avamet.org/mxo-meteoxarxaonline.html"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    patron = re.compile(r"var\s+data\s*=\s*(\[\s*{.*?}\s*\]);", re.DOTALL)
    data_json = None
    for script in soup.find_all("script"):
        m = patron.search(script.text)
        if m:
            data_json = m.group(1)
            break

    if not data_json:
        raise ValueError("No se encontró el objeto 'data' en el HTML")

    estaciones = json.loads(data_json)

    # --- DataFrame ---
    df = pd.DataFrame(estaciones)
    df = df.iloc[:, :19]  # columnas hasta data_ini las demás no nos interesan

    # --- Guardar CSV ---
    dag_folder = os.path.dirname(__file__)  # carpeta donde está el DAG
    output_dir = os.path.join(dag_folder, "data")
    os.makedirs(output_dir, exist_ok=True)   # crea carpeta si no existe

    fecha_str = datetime.now().strftime("%Y-%m-%d_%H%M")
    file_path = os.path.join(output_dir, f"avamet_estaciones_{fecha_str}.csv")
    df.to_csv(file_path, index=False)
    print(f"CSV guardado en: {file_path}, Nº de estaciones: {len(df)}")

# --- DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'AVAMET',
    default_args=default_args,
    description='Scraping horario de AVAMET y guardado en CSV',
    schedule='0 * * * *',  # cada hora, puedes ajustar
    catchup=False
)

# --- Task ---
scrap_task = PythonOperator(
    task_id='scrap_avamet',
    python_callable=scrap_avamet_csv,
    dag=dag
)
