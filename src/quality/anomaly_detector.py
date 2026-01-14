from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.config.spark_config import get_spark_session
from src.config import settings
from src.utils.logging_config import log

class AnomalyDetector:
    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("Anomaly-Detection")

    def detect_statistical_anomalies(self, df, column_to_check):
        """
        Detects anomalies using Z-Score method.
        Records with |Z-Score| > 3 are flagged.
        """
        log.info(f"Running Z-Score anomaly detection on column: {column_to_check}")
        
        try:
            # Calculate global mean and stddev
            stats = df.select(
                F.mean(column_to_check).alias("avg"),
                F.stddev(column_to_check).alias("stddev")
            ).collect()[0]
            
            avg = stats['avg']
            stddev = stats['stddev']
            
            if stddev == 0 or stddev is None:
                log.warning(f"Stddev is zero or null for {column_to_check}, skipping calculation")
                return df.withColumn(f"{column_to_check}_is_anomaly", F.lit(False))

            # Calculate Z-Score
            df_with_zscore = df.withColumn(
                f"{column_to_check}_zscore",
                (F.col(column_to_check) - avg) / stddev
            )
            
            # Flag anomalies
            df_flagged = df_with_zscore.withColumn(
                f"{column_to_check}_is_anomaly",
                F.when(
                    F.abs(F.col(f"{column_to_check}_zscore")) > settings.DQ_ANOMALY_ZSCORE_THRESHOLD, 
                    True
                ).otherwise(False)
            )
            
            anomaly_count = df_flagged.filter(F.col(f"{column_to_check}_is_anomaly") == True).count()
            log.info(f"Detected {anomaly_count} anomalies in {column_to_check}")
            
            return df_flagged

        except Exception as e:
            log.error(f"Error during anomaly detection: {str(e)}")
            return df

    def detect_temporal_drift(self, df, date_col):
        """
        Detects sudden spikes or drops in daily record counts.
        """
        log.info("Detecting temporal drift in record counts")
        
        daily_counts = df.groupBy(date_col).count().orderBy(date_col)
        
        window_spec = Window.orderBy(date_col).rowsBetween(-7, -1) # 7-day moving average
        
        drift_df = daily_counts.withColumn("moving_avg", F.avg("count").over(window_spec)) \
                               .withColumn("moving_std", F.stddev("count").over(window_spec))
        
        # Check if current day's count is significantly different from moving average
        drift_df = drift_df.withColumn(
            "is_drift",
            F.when(
                (F.abs(F.col("count") - F.col("moving_avg")) > 2 * F.col("moving_std")) & (F.col("moving_std") > 0),
                True
            ).otherwise(False)
        )
        
        return drift_df

if __name__ == "__main__":
    detector = AnomalyDetector()
