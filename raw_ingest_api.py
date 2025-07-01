#!/usr/bin/env python3
# raw_ingest_api.py

import time
import requests
from datetime import datetime
from s3clinet import s3, RAW_BUCKET  # j’imagine que s3clinet est ton module interne
from botocore.exceptions import ClientError

API_SOURCES = [
    {
        "name": "colonnes_verre",
        "url": "https://opendata.paris.fr/explore/dataset/dechets-menagers-points-dapport-volontaire-colonnes-a-verre/download/?format=geojson&timezone=Europe%2FBerlin&lang=fr",
        "key_prefix": "api/colonnes_verre",
        "content_type": "application/geo+json"
    },
    {
        "name": "ordures_menageres",
        "url": "https://opendata.paris.fr/explore/dataset/points-d-apport-volontaire-ordures-menageres/download/?format=geojson&timezone=Europe%2FBerlin&lang=fr",
        "key_prefix": "api/ordures_menageres",
        "content_type": "application/geo+json"
    },
    {
        "name": "arrondissements",
        "url": "https://opendata.paris.fr/explore/dataset/arrondissements/download/?format=geojson&timezone=Europe%2FBerlin&lang=fr",
        "key_prefix": "api/arrondissements",
        "content_type": "application/geo+json"
    }
]

INTERVAL_SEC = 24 * 3600  # une fois par jour

def ingest_api():
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    for src in API_SOURCES:
        print(f"[{ts}] Fetching {src['name']} from {src['url']}")
        try:
            resp = requests.get(src["url"], timeout=30)
            resp.raise_for_status()
        except requests.HTTPError as e:
            # Ignore 404 ou autres erreurs HTTP pour cette source
            print(f"⚠️  Erreur HTTP pour {src['name']}: {e}")
            continue
        except requests.RequestException as e:
            # Timeout, DNS, etc.
            print(f"⚠️  Erreur réseau pour {src['name']}: {e}")
            continue

        # Déterminer l’extension
        ext = ".geojson" if "geojson" in src["content_type"] else ".json"
        key = f"{src['key_prefix']}_{ts}{ext}"

        try:
            s3.put_object(
                Bucket=RAW_BUCKET,
                Key=key,
                Body=resp.content,
                ContentType=src["content_type"]
            )
            print(f"[{ts}] Uploaded → s3://{RAW_BUCKET}/{key}")
        except ClientError as e:
            print(f"❌ Erreur S3 pour {src['name']} à l’upload: {e}")
            # on ne break pas, on continue sur les autres sources

if __name__ == "__main__":
    while True:
        ingest_api()
        print(f"En attente de {INTERVAL_SEC} s avant la prochaine itération…")
        time.sleep(INTERVAL_SEC)
