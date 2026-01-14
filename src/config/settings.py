import os
from dotenv import load_dotenv

load_dotenv()

# S3 / MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

# Lakehouse Paths
BRONZE_PATH = "s3a://bronze/healthcare"
SILVER_PATH = "s3a://silver/healthcare"
GOLD_PATH = "s3a://gold/healthcare"

# Spark Configuration
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
APP_NAME = "MediFlow-Lakehouse"

# Data Quality Thresholds
DQ_ANOMALY_ZSCORE_THRESHOLD = 3.0
DQ_FRESHNESS_THRESHOLD_HOURS = 24

# MLflow Configuration
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = "MediFlow-Healthcare-ML"

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_INGESTION = "healthcare-ingestion"

# Security (JWT)
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "mediflow-super-secret-key-123456789")
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
