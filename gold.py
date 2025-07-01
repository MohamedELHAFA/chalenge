#!/usr/bin/env python3
# gold_api_test.py

import os
import boto3
import pandas as pd
from botocore.client import Config

# --- Config MinIO ---
ENDPOINT   = "http://localhost:9000"
KEY        = "minioadmin"
SECRET     = "minioadmin123"
SILVER     = "silver"
GOLD       = "gold"

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# Crée bucket gold s’il n’existe pas
buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
if GOLD not in buckets:
    s3.create_bucket(Bucket=GOLD)

# Répertoire local temporaire
TMP = "tmp_gold_api"
os.makedirs(TMP, exist_ok=True)

# 1) Télécharger containers.parquet depuis silver/api/
silver_key = "api/containers.parquet"
local_src  = os.path.join(TMP, "containers.parquet")
try:
    s3.head_object(Bucket=SILVER, Key=silver_key)
    s3.download_file(SILVER, silver_key, local_src)
    print(f"✓ Downloaded silver/{silver_key}")
except s3.exceptions.NoSuchKey:
    print(f"❌ silver/{silver_key} introuvable. Arrêt.")
    exit(1)

# 2) Lire et enrichir
df = pd.read_parquet(local_src)
# Ajoute une prédiction factice
df["pred_fill_pct"] = 0  # ici tu peux mettre df["pred_fill_pct"] = random ou autre

# 3) Écrire localement et uploader dans gold/api/
local_out = os.path.join(TMP, "containers_with_pred.parquet")
df.to_parquet(local_out, index=False)
gold_key = "api/containers_with_pred.parquet"
s3.upload_file(local_out, GOLD, gold_key)
print(f"→ Uploadé gold/{gold_key}")

print("✅ Test Gold/API terminé.")
