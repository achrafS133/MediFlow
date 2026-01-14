from pyspark.sql import functions as F
from src.config.spark_config import get_spark_session
from src.config import settings
from src.utils.logging_config import log

class GoldAggregator:
    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("Gold-Aggregation")

    def create_patient_summary(self):
        """
        Aggregates Silver data into business-ready Gold layer.
        Example: Patient treatment outcomes summary.
        """
        log.info(f"Starting Gold aggregation from {settings.SILVER_PATH}")
        
        try:
            # 1. Read from Silver
            silver_df = self.spark.read.format("delta").load(settings.SILVER_PATH)
            
            # 2. Business Aggregations
            # Example: Summary by department and gender
            gold_df = silver_df.groupBy("department", "gender", "diagnosis_code") \
                               .agg(
                                   F.count("patient_id").alias("patient_count"),
                                   F.avg("age").alias("avg_age"),
                                   F.sum("cost").alias("total_billing"),
                                   F.avg("cost").alias("avg_billing"),
                                   F.max("admission_date").alias("latest_admission"),
                                   F.min("admission_date").alias("earliest_admission")
                               )
            
            # 3. Add derivation metrics
            gold_df = gold_df.withColumn("report_generation_time", F.current_timestamp())
            
            # 4. Write to Gold Layer
            log.info(f"Writing to Gold layer at {settings.GOLD_PATH}")
            gold_df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy("department") \
                .save(settings.GOLD_PATH)
                
            log.info("Gold aggregation completed successfully")
            return True

        except Exception as e:
            log.error(f"Error during Gold aggregation: {str(e)}")
            return False

if __name__ == "__main__":
    aggregator = GoldAggregator()
    # aggregator.create_patient_summary()
