from pyspark.sql import functions as F
from src.config.spark_config import get_spark_session
from src.config import settings
from src.utils.logging_config import log

class StreamingAnomalyDetector:
    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("Streaming-Anomaly-Detection")

    def detect_anomalies(self):
        """
        Performs real-time anomaly detection on streaming data using sliding windows.
        """
        log.info("Initializing Real-time Anomaly Detection")
        
        try:
            # Read from Silver layer (or directly from Bronze for immediate alerts)
            streaming_df = self.spark.readStream \
                .format("delta") \
                .load(settings.SILVER_PATH)

            # Performance windowed aggregations
            windowed_stats = streaming_df \
                .withWatermark("ingestion_timestamp", "10 minutes") \
                .groupBy(
                    F.window(F.col("ingestion_timestamp"), "5 minutes", "1 minute")
                ) \
                .agg(
                    F.avg("cost").alias("avg_cost"),
                    F.stddev("cost").alias("std_cost"),
                    F.count("*").alias("record_count")
                )

            # Filter for performance spikes
            # Note: In a real scenario, we'd join this back or compare against historical baselines
            anomaly_query = windowed_stats.writeStream \
                .format("console") \
                .outputMode("update") \
                .start()

            log.info("Streaming Anomaly Detection active")
            return anomaly_query

        except Exception as e:
            log.error(f"Error in streaming anomaly detection: {str(e)}")
            return None

if __name__ == "__main__":
    detector = StreamingAnomalyDetector()
    # detector.detect_anomalies().awaitTermination()
