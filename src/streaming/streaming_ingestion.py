from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.config.spark_config import get_spark_session
from src.config import settings
from src.utils.logging_config import log

class StreamingIngestor:
    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("Streaming-Ingestion")
        
        # Define schema for healthcare data
        self.schema = StructType([
            StructField("patient_id", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("department", StringType(), True),
            StructField("cost", DoubleType(), True),
            StructField("diagnosis_code", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

    def start_ingestion(self):
        """
        Starts Spark Structured Streaming from Kafka.
        """
        log.info(f"Starting legacy streaming ingestion from Kafka: {settings.KAFKA_BOOTSTRAP_SERVERS}")
        
        try:
            # Read from Kafka
            raw_stream_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", settings.KAFKA_TOPIC_INGESTION) \
                .option("startingOffsets", "latest") \
                .load()

            # Parse JSON data
            parsed_df = raw_stream_df.selectExpr("CAST(value AS STRING)") \
                .select(F.from_json(F.col("value"), self.schema).alias("data")) \
                .select("data.*") \
                .withColumn("ingestion_timestamp", F.current_timestamp())

            # Write to Bronze layer (Delta Lake)
            query = parsed_df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", "s3a://metadata/checkpoints/bronze_streaming") \
                .start(settings.BRONZE_PATH)

            log.info(f"Streaming ingestion started. Writing to {settings.BRONZE_PATH}")
            return query

        except Exception as e:
            log.error(f"Error starting streaming ingestion: {str(e)}")
            return None

if __name__ == "__main__":
    ingestor = StreamingIngestor()
    # ingestor.start_ingestion().awaitTermination()
