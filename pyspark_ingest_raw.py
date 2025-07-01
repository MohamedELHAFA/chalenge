#!/usr/bin/env python3
# pyspark_ingest_raw.py

import os
import requests
import boto3
from botocore.client import Config
from datetime import datetime
from pyspark.sql import SparkSession

# --- Configuration S3/MinIO (boto3) ---
S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY        = os.getenv("S3_KEY", "minioadmin")
S3_SECRET     = os.getenv("S3_SECRET", "minioadmin123")
RAW_BUCKET    = os.getenv("RAW_BUCKET", "raw")

s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# S'assure que le bucket existe
if RAW_BUCKET not in [b['Name'] for b in s3.list_buckets().get('Buckets', [])]:
    s3.create_bucket(Bucket=RAW_BUCKET)

# Initialise Spark (pour télécharger / lire si besoin)
spark = (
    SparkSession.builder
    .appName("RawIngestPySpark")
    .getOrCreate()
)

def upload_bytes(key: str, data: bytes, content_type: str):
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=data, ContentType=content_type)
    print(f"Uploaded → s3://{RAW_BUCKET}/{key}")

# 1) APIs GeoJSON
API_SOURCES = [
    ("colonnes_verre", "https://opendata.paris.fr/explore/dataset/dechets-menagers-points-dapport-volontaire-colonnes-a-verre/download/?format=geojson"),
    ("ordures_menageres", "https://opendata.paris.fr/explore/dataset/points-d-apport-volontaire-ordures-menageres/download/?format=geojson"),
    ("arrondissements", "https://opendata.paris.fr/explore/dataset/arrondissements/download/?format=geojson"),
]

ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
for name, url in API_SOURCES:
    print(f"Fetching API {name}…")
    resp = requests.get(url)
    resp.raise_for_status()
    key = f"api/{name}_{ts}.geojson"
    upload_bytes(key, resp.content, "application/geo+json")

# 2) Fichier JSON local
local_json = "explore_JSON_with_quantities.json"
if os.path.isfile(local_json):
    with open(local_json, "rb") as f:
        data = f.read()
    key = f"api/quantities_{ts}.json"
    upload_bytes(key, data, "application/json")

# 3) Fichier capteurs
sensor_file = os.path.join("sensor", "sensor_data.txt")
if os.path.isfile(sensor_file):
    with open(sensor_file, "rb") as f:
        data = f.read()
    key = f"sensors/sensor_data_{ts}.txt"
    upload_bytes(key, data, "text/plain")

spark.stop()
