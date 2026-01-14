from pyspark.sql import SparkSession
from src.config import settings

def get_spark_session(app_name=settings.APP_NAME):
    """
    Creates and returns a Spark session with Delta Lake and S3 configurations.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(settings.SPARK_MASTER)
        # Delta Lake Config
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3 / MinIO Config
        .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Optimization & Packages
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    )

    return builder.getOrCreate()
