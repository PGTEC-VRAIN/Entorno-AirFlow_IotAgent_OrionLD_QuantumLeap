import requests

# Configuración
ORION_URL = "http://localhost:1026/v2/subscriptions"
HEADERS = {
    "Fiware-Service": "openiot",
    "Fiware-ServicePath": "/"
}

# --- 1. Listar todas las suscripciones ---
resp = requests.get(ORION_URL, headers=HEADERS)
resp.raise_for_status()

subs = resp.json()
if not subs:
    print("No hay suscripciones activas.")
else:
    print(f"Encontradas {len(subs)} suscripciones:")
    for s in subs:
        print(f"  ID: {s['id']}")

    # --- 2. Eliminar cada suscripción ---
    for s in subs:
        sub_id = s['id']
        del_url = f"{ORION_URL}/{sub_id}"
        r = requests.delete(del_url, headers=HEADERS)
        if r.status_code == 204:
            print(f"✅ Suscripción {sub_id} eliminada.")
        else:
            print(f"❌ Error eliminando {sub_id}: {r.status_code} {r.text}")