import requests
import pandas as pd

# 👉 Sustituye por tu API key
API_KEY = "-khOp77JPZaX1CKKM4E6Q_tGap9jBuzZXAHRBHcSf9g6Y05hVC"

# 👉 Ejemplo: estación 326 (puedes cambiarla por la que quieras)
id_estacion = "A05" #denia
fecha = "2025-09-21"

# Endpoint de datos diarios de una estación
url = f"https://servicio.mapama.gob.es/apisiar/API/v1/Datos/Diarios/Estacion?ClaveAPI={API_KEY}Id={id_estacion}&FechaInicial={fecha}&FechaFinal={fecha}"
params = {
    "fecha": fecha,
    "api_key": API_KEY
}

# Llamada a la API
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()   # La API devuelve JSON
    print("Datos recibidos:")
    print(data)

    # Si quieres guardarlos en CSV con pandas
    df = pd.DataFrame(data)
    df.to_csv("datos_siar_denia.csv", index=False, encoding="utf-8")
    print("Datos guardados en datos_siar.csv")
else:
    print("Error en la petición:", response.status_code, response.text)

