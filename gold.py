#!/usr/bin/env python3
"""
gold_train_model.py

Pipeline Gold en Python pour Smart City Waste Management :
 - Lit les données nettoyées depuis MinIO (`silver`)
 - Entraîne un modèle de régression pour prédire `fill_level`
 - Évalue le modèle (RMSE, R2)
 - Sauvegarde le modèle et les métriques dans le bucket `gold`
 - Génère `sensor_data.txt` et `sensor_position.json` à partir des données Silver

Usage :
    pip install pandas scikit-learn joblib boto3 pyarrow
    python gold_train_model.py
"""
import io
import json
import boto3
import joblib
import pandas as pd
from botocore.client import Config
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error, r2_score

# ----------------------------------------------------------------------
# CONFIGURATION MINIO
# ----------------------------------------------------------------------
SILVER_BUCKET = 'silver'
GOLD_BUCKET   = 'gold'
ENDPOINT_URL  = 'http://localhost:9000'
ACCESS_KEY    = 'minioadmin'
SECRET_KEY    = 'minioadmin123'

# Init client
s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# ----------------------------------------------------------------------
# UTILS
# ----------------------------------------------------------------------
def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj['Body'].read()))


def upload_to_s3(body: bytes, bucket: str, key: str, content_type: str):
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    print(f"✔️  Uploaded s3://{bucket}/{key}")

# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
def main():
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    print(f"🚀 Gold training started at {ts}")

    # 1) Charger les features de Silver
    df = read_parquet_from_s3(SILVER_BUCKET, 'features/features.parquet')

    # 2) Préparation X, y
    feature_cols = ['lat','lon','capacity_tons','annual_tons','daily_tons','hour_of_day','day_of_week']
    X = df[feature_cols]
    y = df['fill_level']

    # 3) Split train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # 4) Pipeline ML
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('rf', RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42))
    ])
    pipeline.fit(X_train, y_train)

    # 5) Évaluation
    y_pred = pipeline.predict(X_test)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    r2   = r2_score(y_test, y_pred)
    metrics = {'rmse': rmse, 'r2': r2, 'timestamp': ts}
    print(f"✅ Evaluation: RMSE={rmse:.3f}, R2={r2:.3f}")

    # 6) Sauvegarde du modèle
    model_key = 'model/fill_level_rf_v1.pkl'
    buf = io.BytesIO()
    joblib.dump(pipeline, buf)
    buf.seek(0)
    upload_to_s3(buf.read(), GOLD_BUCKET, model_key, 'application/octet-stream')

    # 7) Sauvegarde des métriques
    metrics_key = f'metrics/metrics_fill_level_rf_v1_{ts}.json'
    upload_to_s3(json.dumps(metrics, indent=2).encode('utf-8'), GOLD_BUCKET, metrics_key, 'application/json')

    # 8) Génération de sensor_data.txt (prédictions pour la dernière date)
    df_latest = df[df['ts'] == df['ts'].max()].sort_values('sensor_id')
    preds = pipeline.predict(df_latest[feature_cols])
    # Formater en entiers 0-100 par ligne
    lines = [str(int(round(p * 100))) for p in preds]
    sensor_txt = '\n'.join(lines)
    upload_to_s3(sensor_txt.encode('utf-8'), GOLD_BUCKET, 'sensor/sensor_data.txt', 'text/plain')

    # 9) Génération de sensor_position.json à partir de Silver
    df_pos = read_parquet_from_s3(SILVER_BUCKET, 'sensors/positions.parquet')
    positions = df_pos[['lat','lon']].values.tolist()
    sensor_pos_json = json.dumps({'positions': positions}, indent=2)
    upload_to_s3(sensor_pos_json.encode('utf-8'), GOLD_BUCKET, 'sensor/sensor_position.json', 'application/json')

    end = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    print(f"🎉 Gold pipeline completed at {end}")

if __name__ == '__main__':
    import time
    print("⏰ Scheduler démarré — génération de sensor_data.txt toutes les 10 secondes")
    while True:
        main()
        time.sleep(10)
