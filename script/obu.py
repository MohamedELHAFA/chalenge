#!/usr/bin/env python3
"""
obu.py

On-Board Unit (OBU) pour Smart City Waste Management.
Gère dynamiquement N camions, lit les DENM depuis MQTT, calcule les routes
et publie les CAM.

Étapes :
 1. Charger les credentials MinIO via variables d'env
 2. Lire `sensor_position.json` depuis GOLD pour positions capteurs
 3. Définir `TRUCK_COUNT` en fonction du nombre de positions dispo
 4. Initialiser N camions
 5. Souscrire au topic DENM (`vanetza/in/denm`)
 6. Boucle principale : mise à jour des files, dessin de route, publication CAM

Usage :
    export MINIO_ENDPOINT=http://localhost:9000
    export MINIO_ACCESS_KEY=minioadmin
    export MINIO_SECRET_KEY=minioadmin123
    export MAPBOX_TOKEN=<votre_token>  # optionnel
    pip install boto3 paho-mqtt requests
    python obu.py
"""
import os
import json
import time
import math
import boto3
from botocore.client import Config
from datetime import datetime
import requests
import paho.mqtt.client as mqtt

# ----------------------------------------------------------------------
# Configuration MinIO & buckets
# ----------------------------------------------------------------------
MINIO_ENDPOINT   = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
GOLD_BUCKET      = 'gold'
MAPBOX_TOKEN     = os.getenv('MAPBOX_TOKEN', '')  # optionnel

# Init S3 client
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# ----------------------------------------------------------------------
# Charger positions capteurs depuis Gold
# ----------------------------------------------------------------------
obj = s3.get_object(Bucket=GOLD_BUCKET, Key='sensor/sensor_position.json')
sensor_positions = json.loads(obj['Body'].read())['positions']

# Définir nombre de camions selon dispo
TRUCK_COUNT = min(20, len(sensor_positions))

# Position "home" de chaque camion
HOME_TRUCKS = sensor_positions[:TRUCK_COUNT]

# ----------------------------------------------------------------------
# Structures dynamiques pour N camions
# ----------------------------------------------------------------------
truck_positions = [home.copy() for home in HOME_TRUCKS]
queue_trucks    = [[] for _ in range(TRUCK_COUNT)]
current_routes  = [[] for _ in range(TRUCK_COUNT)]
need_recalc     = [False] * TRUCK_COUNT

# ----------------------------------------------------------------------
# MQTT client pour publication CAM
# ----------------------------------------------------------------------
cam_client = mqtt.Client()
cam_client.connect('127.0.0.1', 18830, 60)
cam_client.loop_start()

# ----------------------------------------------------------------------
# Fonction de routage (Mapbox ou fallback)
# ----------------------------------------------------------------------
def draw_route(points):
    """
    Route planning via Mapbox if token present, else direct points.
    """
    if not MAPBOX_TOKEN:
        return points.copy()
    coords = ";".join(f"{p[1]},{p[0]}" for p in points)
    url = f"https://api.mapbox.com/directions/v5/mapbox/driving/{coords}"
    resp = requests.get(url, params={'access_token': MAPBOX_TOKEN, 'geometries':'geojson'})
    resp.raise_for_status()
    data = resp.json()
    # reverse [lon,lat] to [lat,lon]
    return [[lat, lon] for lon, lat in data['routes'][0]['geometry']['coordinates']]

# ----------------------------------------------------------------------
# MQTT client pour souscription DENM
# ----------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):
    print(f"OBU MQTT connected (rc={rc}), subscribing to DENM topic")
    client.subscribe('vanetza/in/denm')


def on_message(client, userdata, msg):
    m = json.loads(msg.payload.decode())
    sub = m.get('management', {}).get('eventType', {}).get('subCauseCode')
    if not isinstance(sub, int) or not (1 <= sub <= TRUCK_COUNT):
        return
    idx = sub - 1
    pos = m.get('situation', {}).get('eventPosition', {})
    lat = pos.get('latitude')
    lon = pos.get('longitude')
    if lat is None or lon is None:
        return
    queue_trucks[idx].append([lat, lon])
    need_recalc[idx] = True
    print(f"DENM received → Truck #{sub} assigned to ({lat:.6f},{lon:.6f})")

sub_client = mqtt.Client()
sub_client.on_connect  = on_connect
sub_client.on_message  = on_message
sub_client.connect('127.0.0.1', 18830, 60)
sub_client.loop_start()

# ----------------------------------------------------------------------
# Boucle principale d'émission CAM
# ----------------------------------------------------------------------
print(f"Starting OBU loop with {TRUCK_COUNT} trucks...")
while True:
    for idx in range(TRUCK_COUNT):
        home   = HOME_TRUCKS[idx]
        queue  = queue_trucks[idx]
        route  = current_routes[idx]

        # recalcul route si demandé ou route vide
        if need_recalc[idx] or not route:
            if queue:
                route = draw_route(queue)
                current_routes[idx] = route
            need_recalc[idx] = False

        # choisir waypoint courant
        if route:
            waypoint = route.pop(0)
            if not route:
                queue_trucks[idx] = []
        else:
            waypoint = home

        # construire CAM
        cam = {
            'stationID': idx+1,
            'latitude' : waypoint[0],
            'longitude': waypoint[1],
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        cam_client.publish('vanetza/in/cam', json.dumps(cam))
        time.sleep(0.2)

    time.sleep(1)
