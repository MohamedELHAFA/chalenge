import os, glob, json, boto3, pandas as pd
from botocore.client import Config

# Config MinIO
s3 = boto3.client("s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    config=Config(signature_version="s3v4")
)

# 1) Télécharger raw/api/*.json en tmp_api/
os.makedirs("tmp_api", exist_ok=True)
for obj in s3.list_objects_v2(Bucket="raw", Prefix="api/")["Contents"]:
    key = obj["Key"]
    if key.lower().endswith((".json", ".geojson")):
        local = os.path.join("tmp_api", os.path.basename(key))
        s3.download_file("raw", key, local)

# 2) Lire chaque GeoJSON, exploser les features, agréger dans un DataFrame
rows = []
for f in glob.glob("tmp_api/*.*json"):
    src = os.path.basename(f).split("_",1)[0]
    gj = json.load(open(f))
    for feat in gj["features"]:
        lon, lat = feat["geometry"]["coordinates"]
        rows.append({"source": src, "latitude": lat, "longitude": lon})
df = pd.DataFrame(rows)

# 3) Écrire en Parquet local et uploader vers silver/api/
os.makedirs("tmp_silver/api", exist_ok=True)
out = "tmp_silver/api/containers.parquet"
df.to_parquet(out, index=False)
s3.upload_file(out, "silver", "api/containers.parquet")
