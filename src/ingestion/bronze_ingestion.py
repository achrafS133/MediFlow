import os
from pyspark.sql import functions as F
from src.config.spark_config import get_spark_session
from src.config import settings
from src.utils.logging_config import log

class BronzeIngestor:
    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("Bronze-Ingestion")

    def ingest_raw_data(self, source_path, source_system):
        """
        Reads raw data from source and writes to Bronze layer with metadata.
        """
        log.info(f"Starting ingestion from {source_path} for system {source_system}")
        
        try:
            # Determine file format based on extension
            file_ext = os.path.splitext(source_path)[1].lower()
            
            if file_ext == '.csv':
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            elif file_ext == '.json':
                df = self.spark.read.json(source_path)
            else:
                log.error(f"Unsupported file format: {file_ext}")
                return False

            # Add ingestion metadata
            df_with_metadata = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
                                 .withColumn("source_system", F.lit(source_system)) \
                                 .withColumn("ingestion_date", F.to_date(F.col("ingestion_timestamp")))

            # Write to Bronze layer as Parquet, partitioned by ingestion_date
            log.info(f"Writing data to Bronze layer at {settings.BRONZE_PATH}")
            df_with_metadata.write.mode("append") \
                .partitionBy("ingestion_date") \
                .parquet(settings.BRONZE_PATH)
            
            log.info("Ingestion to Bronze layer completed successfully")
            return True

        except Exception as e:
            log.exception(f"Error during Bronze ingestion: {str(e)}")
            return False

if __name__ == "__main__":
    ingestor = BronzeIngestor()
    # Example usage:
    # ingestor.ingest_raw_data("/app/data/raw/patients.csv", "EHR_SYSTEM_A")
