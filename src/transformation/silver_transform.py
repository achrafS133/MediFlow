from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from src.config.spark_config import get_spark_session
from src.config import settings
from src.utils.logging_config import log

class SilverTransformer:
    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("Silver-Transformation")

    def clean_and_normalize(self):
        """
        Reads from Bronze, cleanses, deduplicates, and writes to Delta-based Silver layer.
        """
        log.info(f"Starting Silver transformation from {settings.BRONZE_PATH}")
        
        try:
            # 1. Read from Bronze
            bronze_df = self.spark.read.parquet(settings.BRONZE_PATH)
            
            # 2. Basic Cleansing & Standardization
            # Filtering out rows without essential IDs
            cleansed_df = bronze_df.filter(F.col("patient_id").isNotNull())
            
            # Standardizing case and trimming whitespace
            for col_name in [c for c, t in cleansed_df.dtypes if t == "string"]:
                cleansed_df = cleansed_df.withColumn(col_name, F.trim(F.upper(F.col(col_name))))
            
            # 3. Deduplication (Keeping latest record based on ingestion_timestamp)
            window_spec = Window.partitionBy("patient_id").orderBy(F.col("ingestion_timestamp").desc())
            deduplicated_df = cleansed_df.withColumn("rn", F.row_number().over(window_spec)) \
                                         .filter(F.col("rn") == 1) \
                                         .drop("rn")
            
            # 4. Write to Silver Layer (Delta Table)
            log.info(f"Writing to Silver layer at {settings.SILVER_PATH}")
            
            # Check if Delta Table exists to perform UPSERT (Merge)
            if DeltaTable.isDeltaTable(self.spark, settings.SILVER_PATH):
                log.info("Performing MERGE operation into Silver Delta table")
                silver_table = DeltaTable.forPath(self.spark, settings.SILVER_PATH)
                
                silver_table.alias("target").merge(
                    deduplicated_df.alias("source"),
                    "target.patient_id = source.patient_id"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
            else:
                log.info("Creating NEW Silver Delta table")
                deduplicated_df.write.format("delta") \
                    .mode("overwrite") \
                    .partitionBy("source_system") \
                    .save(settings.SILVER_PATH)
            
            # 5. Optimize Delta Table (Z-Ordering)
            log.info("Optimizing Silver Delta table with Z-Order on patient_id")
            self.spark.sql(f"OPTIMIZE delta.`{settings.SILVER_PATH}` ZORDER BY (patient_id)")
                
            log.info("Silver transformation completed successfully")
            return True

        except Exception as e:
            log.error(f"Error during Silver transformation: {str(e)}")
            return False

if __name__ == "__main__":
    transformer = SilverTransformer()
    # transformer.clean_and_normalize()
