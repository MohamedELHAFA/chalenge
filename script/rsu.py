import os
import json
import time
import math
import threading
import paho.mqtt.client as mqtt

# Pourcentage à partir duquel on considère la poubelle pleine
WARNING_PERCENTAGE = 70

# Seuil max de poubelles qu’un camion peut prendre (en % du total)
THRESHOLD_PERCENTAGE = 75
threshold = THRESHOLD_PERCENTAGE / 100

# Répertoire de base (script/)
BASE_DIR = os.path.dirname(__file__)

# Chargement des coordonnées des poubelles
# → on utilise sensor_position.json (singulier)
sensor_path = os.path.normpath(
    os.path.join(BASE_DIR, '..', 'sensor', 'sensor_position.json')
)
if not os.path.isfile(sensor_path):
    raise FileNotFoundError(f"Fichier introuvable : {sensor_path}")

with open(sensor_path, "r") as f:
    sensor_positions = json.load(f)
GARBAGE_COORDINATES = sensor_positions['positions']

# Pour stocker la dernière position connue de chaque camion
truck_positions = [
    [None, None],
    [None, None],
    [None, None],
]

# Compteurs de poubelles assignées par camion
truck_assigned_counts = [0, 0, 0]
total_assigned = 0

def on_connect(client, userdata, flags, rc):
    print(f"RSU MQTT connecté (rc={rc})")
    client.subscribe("vanetza/out/cam")

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    station = payload['stationID']
    lat, lon = payload['latitude'], payload['longitude']

    # Écriture du JSON CAM pour le dashboard
    out_path = os.path.normpath(
        os.path.join(BASE_DIR, '..', 'dashboard', 'static', f"out_cam_obu{station}.json")
    )
    with open(out_path, "w") as f:
        json.dump(payload, f)

    print(f"CAM reçu → Camion #{station} @ ({lat:.6f}, {lon:.6f})")
    truck_positions[station - 1] = [lat, lon]

def generate_denm(garbage_id, lat, lon, client):
    global total_assigned
    # Prépare le message DENM
    denm_template = os.path.join(BASE_DIR, 'in_denm.json')
    with open(denm_template, 'r') as f:
        m = json.load(f)

    m['management']['actionID']['originatingStationID'] = garbage_id + 1
    m['management']['eventPosition']['latitude']  = lat
    m['management']['eventPosition']['longitude'] = lon
    m['situation']['eventType']['causeCode']     = 50

    # Choix du camion le plus proche, sous seuil
    nearest, min_dist = None, float('inf')
    for idx, pos in enumerate(truck_positions):
        if None in pos:
            return False
        if truck_assigned_counts[idx] > total_assigned * threshold:
            continue
        d = math.dist((lat, lon), tuple(pos))
        if d < min_dist:
            min_dist, nearest = d, idx

    if nearest is None:
        return False

    m['situation']['eventType']['subCauseCode'] = nearest + 1
    truck_assigned_counts[nearest] += 1
    total_assigned += 1

    client.publish("vanetza/in/denm", json.dumps(m))
    return True

# Création et connexion du client MQTT au broker VANETZA RSU
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connexion sur le broker exposé en localhost:18830
client.connect("127.0.0.1", 18830, 60)

# Démarrage du loop MQTT en thread
threading.Thread(target=client.loop_forever, daemon=True).start()

# Boucle principale : lecture des capteurs et génération des DENM
while True:
    data_file = os.path.normpath(os.path.join(BASE_DIR, '..', 'sensor', 'sensor_data.txt'))
    with open(data_file, 'r') as f:
        lines = f.readlines()

    for sid, fill in enumerate(lines):
        pct = int(fill.strip())
        if pct > WARNING_PERCENTAGE:
            lat, lon = GARBAGE_COORDINATES[sid]
            if generate_denm(sid, lat, lon, client):
                # reset du capteur
                lines[sid] = "0\n"
                with open(data_file, 'w') as fw:
                    fw.writelines(lines)
            else:
                print("Aucun camion dispo pour collecter.")
            time.sleep(0.3)

    time.sleep(1)
