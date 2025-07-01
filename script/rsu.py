#!/usr/bin/env python3
"""
rsu.py

Road-Side Unit (RSU) pour Smart City Waste Management.
Lit continuellement les prédictions de remplissage depuis Gold,
génère les alertes DENM vers OBU en MQTT pour N camions.

Étapes :
 1. Charger les credentials MinIO via variables d'env
 2. Lire `sensor_position.json` depuis GOLD pour positions capteurs
 3. Définir `TRUCK_COUNT` en fonction du nombre de positions des camions
 4. Initialiser les structures dynamiques pour N camions
 5. Souscrire aux CAM (`vanetza/out/cam`) pour mettre à jour les positions des camions
 6. Boucle principale : lire `sensor_data.txt`, pour chaque capteur au-dessus du seuil,
    générer un DENM en choisissant le camion le plus proche et publier MQTT
    puis remettre à zéro la ligne dans `sensor_data.txt` et réécrire dans GOLD

Usage :
    export MINIO_ENDPOINT=http://localhost:9000
    export MINIO_ACCESS_KEY=minioadmin
    export MINIO_SECRET_KEY=minioadmin123
    pip install boto3 paho-mqtt
    python rsu.py
"""
import os
import json
import time
import math
import threading
import boto3
from botocore.client import Config
import paho.mqtt.client as mqtt
from datetime import datetime

# ----------------------------------------------------------------------
# Configuration MinIO via variables d'environnement
# ----------------------------------------------------------------------
MINIO_ENDPOINT   = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
GOLD_BUCKET      = 'gold'

# Init S3 client
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# ----------------------------------------------------------------------
# Seuils métier
# ----------------------------------------------------------------------
WARNING_THRESHOLD = 0.70  # 70% fill triggers DENM
MAX_ASSIGN_RATIO  = 0.75  # max assignments per truck (ratio of total assignments)

# ----------------------------------------------------------------------
# Charger positions capteurs depuis GOLD_BUCKET
# ----------------------------------------------------------------------
obj = s3.get_object(Bucket=GOLD_BUCKET, Key='sensor/sensor_position.json')
sensor_positions = json.loads(obj['Body'].read())['positions']

# ----------------------------------------------------------------------
# Définir nombre de camions
# ----------------------------------------------------------------------
# Typiquement, TRUCK_COUNT correspond au nombre de positions capteurs que vous voulez gérer
TRUCK_COUNT = len(sensor_positions)

# ----------------------------------------------------------------------
# Initialiser structures dynamiques pour N camions
# ----------------------------------------------------------------------
truck_positions    = [ [None, None] for _ in range(TRUCK_COUNT) ]
truck_assign_count = [0] * TRUCK_COUNT
total_assigned     = 0

# ----------------------------------------------------------------------
# MQTT callbacks pour CAM from OBU
# ----------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):
    print(f"RSU MQTT connecté (rc={rc}) — abonnement aux CAM")
    client.subscribe("vanetza/out/cam")


def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    station = payload.get('stationID')
    if not isinstance(station, int) or not (1 <= station <= TRUCK_COUNT):
        print("⚠️ CAM invalide, stationID manquant ou hors limites", station)
        return
    # extraction coords robuste
    if 'positionVector' in payload:
        lat = payload['positionVector'].get('latitude')
        lon = payload['positionVector'].get('longitude')
    else:
        lat = payload.get('latitude')
        lon = payload.get('longitude')
    if lat is None or lon is None:
        print("⚠️ Coordonnées manquantes dans CAM", payload.keys())
        return
    truck_positions[station-1] = [lat, lon]
    print(f"CAM reçu → Truck #{station} @ ({lat:.6f},{lon:.6f})")

# ----------------------------------------------------------------------------
# Génération du DENM
# ----------------------------------------------------------------------------
def generate_denm(sensor_idx, lat, lon, client):
    global total_assigned
    tpl = json.load(open('in_denm.json'))
    tpl['management']['actionID']['originatingStationID'] = sensor_idx + 1
    tpl['situation']['eventType']['causeCode'] = 50
    tpl['situation']['eventPosition'] = {'latitude': lat, 'longitude': lon}
    tpl['situation']['startTime'] = datetime.utcnow().isoformat() + 'Z'

    # choisir camion le plus proche et disponible
    nearest, min_dist = None, float('inf')
    for idx, pos in enumerate(truck_positions):
        if None in pos: continue
        if truck_assign_count[idx] > total_assigned * MAX_ASSIGN_RATIO: continue
        d = math.dist((lat, lon), tuple(pos))
        if d < min_dist:
            min_dist, nearest = d, idx
    if nearest is None:
        return False
    tpl['management']['eventType']['subCauseCode'] = nearest + 1
    truck_assign_count[nearest] += 1
    total_assigned += 1
    client.publish("vanetza/in/denm", json.dumps(tpl))
    print(f"DENM généré pour capteur #{sensor_idx+1}, assigné Truck #{nearest+1}")
    return True

# ----------------------------------------------------------------------------
# Start MQTT client for CAM
# ----------------------------------------------------------------------------
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect('127.0.0.1', 18830, 60)
threading.Thread(target=client.loop_forever, daemon=True).start()

# ----------------------------------------------------------------------------
# Boucle principale : lecture et traitement des prédictions
# ----------------------------------------------------------------------------
print(f"RSU démarré pour {TRUCK_COUNT} trucks, seuil DENM={WARNING_THRESHOLD*100}%")
while True:
    obj = s3.get_object(Bucket=GOLD_BUCKET, Key='sensor/sensor_data.txt')
    lines = obj['Body'].read().decode('utf-8').splitlines()
    for idx, fill_str in enumerate(lines):
        fill = int(fill_str)
        pct = fill / 100.0
        if pct > WARNING_THRESHOLD:
            lat, lon = sensor_positions[idx]
            if generate_denm(idx, lat, lon, client):
                lines[idx] = '0'
                s3.put_object(
                    Bucket=GOLD_BUCKET,
                    Key='sensor/sensor_data.txt',
                    Body='\n'.join(lines).encode('utf-8'),
                    ContentType='text/plain'
                )
            else:
                print(f"Aucun truck dispo pour capteur #{idx+1}")
            time.sleep(0.3)
    time.sleep(1)
