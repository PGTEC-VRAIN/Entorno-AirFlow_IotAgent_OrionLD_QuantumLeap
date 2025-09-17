# Entorno-AirFlow_IotAgent_OrionLD_QuantumLeap
Este repositorio contiene un entorno completo para orquestación y gestión de datos IoT utilizando los siguientes componentes:

🔹 Componentes principales

Apache Airflow: Orquestación de flujos de trabajo (DAGs) para automatizar procesos.

IoT Agent: Interfaz para recibir datos de dispositivos IoT y enviarlos al contexto adecuado.

Orion LD: Motor de gestión de contexto basado en NGSI-LD.

QuantumLeap: Servicio de almacenamiento histórico de eventos y datos temporales para análisis.

🔹 Contenido del repositorio

docker-compose.yml → Configuración de los servicios y contenedores.

dags/ → Directorio con los DAGs de Airflow. 

Scripts:

- AEMET.py: Accede a los datos de la AEMET via API y los descarga

- Copernicus.py: Accede a los datos de copernicus de la província de Valencia via CDSAPI de python

- Flujo_Copernicus_orion.py: DAG que accede a los datos de copernicus via API, los registra el IoT Agent y los almacena el context broker Orion LD

- Flujo_copernicus_orion_quantumleap.py: Dag anterior pero mejorada al tener quantumleap para almacenar datos históricos mediante CrateDB (Se recomienda lanzar esta DAG)

🔹 Cómo levantar el entorno

Clonar el repositorio:

git clone https://github.com/PGTEC-VRAIN/Entorno-AirFlow_IotAgent_OrionLD_QuantumLeap.git

cd Entorno-AirFlow_IotAgent_OrionLD_QuantumLeap

Para levantar los contenedores:

docker-compose up -d --build

La opción --build se usa solamente si se modificado el yaml

La opción -d se usa para evitar generar los logs en la terminal

Se recomienda usar "docker ps" para ver el estado de los contenedores

Para finalizar la ejecución usar "docker compose down" en una nueva terminal o clicar Control + c para interrumpir la terminal actual
