#!/usr/bin/env python3
"""
silver_etl.py

Pipeline Silver en Python (pandas + boto3) pour Smart City Waste Management.

Étapes :
 1. Récupérer les données brutes depuis MinIO (`raw`)
 2. Nettoyer et transformer (JSON, JSONL, CSV, GeoJSON)
 3. Calculer les métriques métier et assembler le jeu de features
 4. Écrire les Parquets dans le bucket `silver`

Usage :
    pip install pandas boto3 pyarrow
    python silver_etl.py
"""
import io, json, boto3, pandas as pd
from botocore.client import Config
from datetime import datetime

# ----------------------------------------------------------------------
# CONFIGURATION MINIO
# ----------------------------------------------------------------------
RAW_BUCKET    = "raw"
SILVER_BUCKET = "silver"
ENDPOINT_URL  = "http://localhost:9000"
ACCESS_KEY    = "minioadmin"
SECRET_KEY    = "minioadmin123"
CAPACITY_TONS = 0.12  # 120 L = 0.12 t par poubelle

# Init client
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4")
)

# ----------------------------------------------------------------------
# UTILITAIRES
# ----------------------------------------------------------------------
def list_keys(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def read_json(bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def read_csv(bucket, key):
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    text = raw.decode("utf-8", errors="ignore")
    sep = ";" if ";" in text.splitlines()[0] and "," not in text.splitlines()[0] else ","
    return pd.read_csv(io.BytesIO(raw), sep=sep, engine="python")


def read_jsonl(bucket, prefix):
    dfs = []
    for key in list_keys(bucket, prefix):
        if key.lower().endswith(".jsonl"):
            data = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
            dfs.append(pd.read_json(io.StringIO(data), lines=True))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def upload_parquet(df, bucket, key):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.read())
    print(f"✔️  s3://{bucket}/{key}")

# ----------------------------------------------------------------------
# ETL SILVER
# ----------------------------------------------------------------------
def main():
    start = datetime.utcnow().isoformat()
    print(f"🚀 Silver ETL démarré à {start}")

    # 1) Positions capteurs
    pos = read_json(RAW_BUCKET, "sensor/sensor_position.json")
    df_pos = pd.DataFrame(pos.get("positions", []), columns=["lat","lon"])
    df_pos["sensor_id"] = [f"S{i+1}" for i in range(len(df_pos))]
    df_pos["capacity_tons"] = CAPACITY_TONS
    upload_parquet(df_pos, SILVER_BUCKET, "sensors/positions.parquet")

    # 2) Historique fill levels
    df_hist = read_jsonl(RAW_BUCKET, "sensor/historic_fill_levels/")
    if not df_hist.empty:
        df_hist["ts"] = pd.to_datetime(df_hist["timestamp"], unit="ms")
        upload_parquet(
            df_hist[["sensor_id","ts","fill_level"]],
            SILVER_BUCKET, "sensors/historic_fill.parquet"
        )

    # 3) Population INSEE
    df_pop = read_csv(RAW_BUCKET, "demographics/populations_legales_2021.csv")
    cols = df_pop.columns.tolist()
    # détecter colonnes arrdt et population
    code_candidates = [c for c in cols if c.lower().startswith("codarr") or c.lower()=="arr" or "arrondissement" in c.lower()]
    pop_candidates  = [c for c in cols if c.lower() in ("pmun","ptot") or "pop" in c.lower()]
    if not code_candidates or not pop_candidates:
        raise RuntimeError(f"CSV INSEE, colonnes introuvables parmi {cols}")
    code_col = code_candidates[0]
    pop_col  = pop_candidates[0]
    df_pop = df_pop[[code_col, pop_col]].rename(
        columns={code_col: "arrondissement_id", pop_col: "population"}
    )
    upload_parquet(df_pop, SILVER_BUCKET, "demographics/population.parquet")

    # 4) PAV points (tous api/* sauf tonnage)
    pav = []
    for key in list_keys(RAW_BUCKET, "api/"):
        if key.lower().endswith(".geojson") and "tonnage_par_habitant" not in key:
            pav_type = key.split('/')[1]
            gj = read_json(RAW_BUCKET, key)
            for feat in gj.get("features", []):
                lon, lat = feat["geometry"]["coordinates"]
                pav.append({"pav_type": pav_type, "lat": lat, "lon": lon})
    if pav:
        upload_parquet(pd.DataFrame(pav), SILVER_BUCKET, "pav/points.parquet")

        # 5) Tonnage par habitant → annual_tons city-wide
    ton_keys = [k for k in list_keys(RAW_BUCKET, "api/tonnage_par_habitant/") if k.lower().endswith(".geojson")]
    if ton_keys:
        last = sorted(ton_keys)[-1]
        ton_json = read_json(RAW_BUCKET, last)
        props = [feat.get("properties", {}) for feat in ton_json.get("features", [])]
        # Somme des quantites (kg/habitant) sur toutes catégories
        kg_per_inhabitant = sum(p.get("quantite", 0) for p in props)
        # Population totale city-wide
        total_pop = df_pop["population"].sum()
        # Tonnage annuel total de la ville
        total_annual_tons = kg_per_inhabitant * total_pop / 1000.0
        # On stocke une table simple avec ce KPI
        df_ton_city = pd.DataFrame([{
            "kg_per_inhabitant": kg_per_inhabitant,
            "total_population": total_pop,
            "city_annual_tons": total_annual_tons
        }])
        upload_parquet(df_ton_city, SILVER_BUCKET, "tonnage/city_aggregate.parquet")
    else:
        print("⚠️  Aucun tonnage_par_habitant trouvé en Raw")

    # 6) Proxy tonnage par capteur (uniforme)
    # Si la table df_ton_city existe, on lit city_annual_tons
    if 'total_annual_tons' in locals() and total_annual_tons>0:
        n = len(df_pos)
        annual_ps = total_annual_tons / n
        daily_ps  = annual_ps / 365.0
    else:
        annual_ps = daily_ps = 0.0

        print("⚠️  Pas de tonnage_par_habitant trouvé en Raw")

    # 6) Proxy tonnage par capteur
    if 'df_ton' in locals() and not df_ton.empty:
        total = df_ton["annual_tons"].sum()
        n = len(df_pos)
        annual_ps = total / n
        daily_ps  = annual_ps / 365.0
    else:
        annual_ps = daily_ps = 0.0
    df_proxy = pd.DataFrame({
        "sensor_id": df_pos["sensor_id"],
        "annual_tons": annual_ps,
        "daily_tons":  daily_ps
    })
    upload_parquet(df_proxy, SILVER_BUCKET, "sensor_tonnage/proxy.parquet")

    # 7) Jeu de features
    if not df_hist.empty:
        df_feat = (
            df_hist
            .merge(df_pos[["sensor_id","lat","lon","capacity_tons"]], on="sensor_id", how="left")
            .merge(df_proxy, on="sensor_id", how="left")
        )
        df_feat["hour_of_day"] = df_feat["ts"].dt.hour
        df_feat["day_of_week"] = df_feat["ts"].dt.dayofweek
        final = df_feat[[
            "sensor_id","ts","lat","lon","capacity_tons",
            "annual_tons","daily_tons","hour_of_day","day_of_week","fill_level"
        ]]
        upload_parquet(final, SILVER_BUCKET, "features/features.parquet")

    end = datetime.utcnow().isoformat()
    print(f"🎉 Silver ETL terminé à {end}")

if __name__=="__main__":
    main()
